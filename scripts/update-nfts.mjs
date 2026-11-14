// Daily update of data/nfts.json. Runs in GitHub Actions (see .github/workflows/update-nfts.yml).
// Node 20+, needs `sharp`, `ffmpeg` and the GitHub CLI `gh`.
//
// 1. Reads every ERC-721 / ERC-1155 token held by the wallet from Blockscout, minus data/exclude.json.
// 2. With an OPENSEA_API_KEY secret, asks OpenSea for each token's current image (dynamic NFTs such as
//    LAYERS change over time, and some original hosts are gone) and for collection creators.
// 3. Names the artist from data/artists.json, then the token metadata, then the collection name.
// 4. Makes a small .webp thumbnail per token in thumbs/ (first frame for videos).
// 5. Keeps a full-resolution copy of every image and video as assets of the GitHub release "archive"
//    (listed in data/archive.json), so the originals survive even if their hosts close.
// 6. Writes data/artists-report.json: what OpenSea says about who made each collection, for checking.
// If Blockscout cannot be reached, the script exits without touching existing data.

import fs from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import sharp from 'sharp';

const run = promisify(execFile);
const WALLET = '0xb884ad7ab6af74693fec7f364eb8bfb31fc7ecea';
const API = `https://eth.blockscout.com/api/v2/addresses/${WALLET}`;
const OS_API = 'https://api.opensea.io/api/v2';
const OS_KEY = process.env.OPENSEA_API_KEY || '';
const REPO = process.env.GITHUB_REPOSITORY || 'chromospaceart/chromospace';
const RELEASE = 'archive';
const ARCHIVE_ON = process.env.GITHUB_ACTIONS === 'true' && !!process.env.GH_TOKEN; // only inside GitHub Actions
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const DATA = path.join(ROOT, 'data');
const THUMBS = path.join(ROOT, 'thumbs');
const MAX_PAGES = 60;
const MAX_NEW_THUMBS = 400;
const PARALLEL = 6;
const THUMB_WIDTH = 640;
const ARCHIVE_MINUTES = 45;          // time budget per run; the rest continues the next night
const ARCHIVE_MAX_BYTES = 1.5e9;     // GitHub release assets must stay under 2 GB
const UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 chromospace-bot';
const GATEWAYS = ['https://ipfs.io/ipfs/', 'https://w3s.link/ipfs/', 'https://dweb.link/ipfs/', 'https://gateway.pinata.cloud/ipfs/', 'https://4everland.io/ipfs/'];
// Contracts shared by many artists: the creator is looked up per token.
const SHARED = new Set([
  '0x3b3ee1931dc30c1957379fac9aba94d1c48a5405', // Foundation
  '0xabb3738f04dc2ec20f4ae4462c3d069d02ae045b', // KnownOrigin
  '0x495f947276749ce646f68ac8c248420045cb7b5e', // OpenSea Shared Storefront
  '0x7c78f67700e700b4005c8a3d920a1a99e6004800'  // Art Blocks Studio
]);

const sleep = ms => new Promise(r => setTimeout(r, ms));
const lower = s => String(s || '').toLowerCase();
const tokenAddr = t => lower(t && (t.address_hash || t.address || t.hash));
const started = Date.now();

async function getJSON(url, headers = {}, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url, { headers: { accept: 'application/json', 'user-agent': UA, ...headers }, signal: AbortSignal.timeout(30000) });
      if (res.status === 429) { await sleep(4000 * (i + 1)); continue; }
      if (res.status === 404) return null;
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return await res.json();
    } catch (e) {
      if (i === tries - 1) throw e;
      await sleep(2000 * (i + 1));
    }
  }
  throw new Error(`Gave up on ${url}`);
}
const osGet = p => getJSON(OS_API + p, { 'x-api-key': OS_KEY });

async function getAll(endpoint, extra = {}) {
  const base = { type: 'ERC-721,ERC-1155', ...extra };
  let params = new URLSearchParams(base);
  const out = [];
  for (let p = 0; p < MAX_PAGES; p++) {
    const j = await getJSON(`${API}${endpoint}?${params}`);
    out.push(...((j && j.items) || []));
    if (!j || !j.next_page_params) break;
    params = new URLSearchParams({ ...base, ...j.next_page_params });
    await sleep(250);
  }
  return out;
}

/* ---------- URLs ---------- */
function ipfsPath(u) {
  if (u.startsWith('ipfs://')) return u.slice(7).replace(/^ipfs\//, '');
  const m = u.match(/^https?:\/\/[^/]+\/ipfs\/(.+)$/);
  if (m) return m[1];
  const sub = u.match(/^https?:\/\/(b[a-z2-7]{50,})\.ipfs\.[^/]+\/?(.*)$/);
  return sub ? sub[1] + (sub[2] ? '/' + sub[2] : '') : null;
}
function candidates(u) {
  if (!u || typeof u !== 'string') return [];
  if (u.startsWith('data:')) return [u];
  if (u.startsWith('ar://')) return ['https://arweave.net/' + u.slice(5)];
  const p = ipfsPath(u);
  if (p) return [...new Set([u.startsWith('http') ? u : null, ...GATEWAYS.map(g => g + p)].filter(Boolean))];
  return [u];
}
function publicUrl(u) {
  if (!u) return '';
  const p = ipfsPath(u);
  const url = p ? GATEWAYS[0] + p : (candidates(u)[0] || '');
  return url.startsWith('data:') && url.length > 4000 ? '' : url;
}
// OpenSea CDN links carry a size; without it they return the full image.
const fullSize = u => (u && /seadn\.io/.test(u)) ? u.split('?')[0] : u;

/* ---------- Artists ---------- */
function artistFor(md, collection, contract, key, map) {
  if (map.tokens[key]) return map.tokens[key];
  if (map.contracts[contract]) return map.contracts[contract];
  const attr = Array.isArray(md.attributes) && md.attributes.find(a => /^(artist|creator|created by|artist name)$/i.test(String(a.trait_type || '')));
  const fromMd = md.artist || md.created_by || md.creator || md.artist_name || (attr && attr.value);
  if (fromMd && typeof fromMd === 'string' && !/^0x[0-9a-f]{40}$/i.test(fromMd.trim())) return fromMd.trim();
  if (map.collections[collection]) return map.collections[collection];
  const by = collection.match(/\bby\s+(.+)$/i);
  if (by) return by[1].trim();
  return collection || 'Unattributed';
}

/* ---------- Downloads ---------- */
async function download(url) {
  if (url.startsWith('data:')) {
    const comma = url.indexOf(',');
    const meta = url.slice(5, comma), body = url.slice(comma + 1);
    return { buf: meta.includes(';base64') ? Buffer.from(body, 'base64') : Buffer.from(decodeURIComponent(body)), type: meta.split(';')[0] };
  }
  const res = await fetch(url, { headers: { 'user-agent': UA, accept: 'image/*,video/*,*/*;q=0.8' }, redirect: 'follow', signal: AbortSignal.timeout(60000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const type = (res.headers.get('content-type') || '').toLowerCase();
  if (/text\/html|json/.test(type)) throw new Error(`not an image (${type})`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length > 150 * 1024 * 1024) throw new Error('file too large for a thumbnail');
  return { buf, type };
}
async function toWebp(input, file) {
  await sharp(input, { animated: false, limitInputPixels: 4e8 })
    .rotate()
    .resize({ width: THUMB_WIDTH, height: Math.round(THUMB_WIDTH * 1.25), fit: 'inside', withoutEnlargement: true })
    .webp({ quality: 80 })
    .toFile(file);
}
async function videoFrame(buf) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'cs-'));
  const src = path.join(dir, 'in'), out = path.join(dir, 'frame.png');
  try {
    await fs.writeFile(src, buf);
    try { await run('ffmpeg', ['-v', 'error', '-y', '-ss', '1', '-i', src, '-frames:v', '1', out], { timeout: 90000 }); }
    catch { await run('ffmpeg', ['-v', 'error', '-y', '-i', src, '-frames:v', '1', out], { timeout: 90000 }); }
    return await fs.readFile(out);
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
}
const thumbFailures = [];
async function makeThumb(sources, file, label) {
  const reasons = [];
  for (const src of sources) {
    for (const url of candidates(src)) {
      try {
        const { buf, type } = await download(url);
        try { await toWebp(buf, file); }
        catch (e) {
          if (/video|octet-stream|mp4|quicktime|webm/.test(type) || !type) await toWebp(await videoFrame(buf), file);
          else throw e;
        }
        return url;
      } catch (e) { reasons.push(`${url.slice(0, 70)} → ${e.message}`); }
    }
  }
  thumbFailures.push(`${label}: ${reasons.slice(0, 3).join(' | ') || 'no image url'}`);
  return '';
}
async function pool(tasks, n) {
  let i = 0;
  await Promise.all(Array.from({ length: n }, async () => { while (i < tasks.length) { const t = tasks[i++]; await t(); } }));
}

/* ---------- Archive of originals (GitHub release assets) ---------- */
const EXT = { 'image/png': 'png', 'image/jpeg': 'jpg', 'image/jpg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp', 'image/svg+xml': 'svg', 'image/avif': 'avif',
  'video/mp4': 'mp4', 'video/quicktime': 'mov', 'video/webm': 'webm', 'audio/mpeg': 'mp3', 'audio/wav': 'wav', 'model/gltf-binary': 'glb', 'model/gltf+json': 'gltf' };
function extFor(type, url) {
  if (EXT[type]) return EXT[type];
  const m = String(url).split('?')[0].match(/\.([a-z0-9]{2,5})$/i);
  return m ? m[1].toLowerCase() : 'bin';
}
async function saveOriginal(url, dir, base) {
  if (url.startsWith('data:')) {
    const { buf, type } = await download(url);
    const file = path.join(dir, `${base}.${extFor(type, '')}`);
    await fs.writeFile(file, buf);
    return { file, bytes: buf.length };
  }
  const res = await fetch(url, { headers: { 'user-agent': UA }, redirect: 'follow', signal: AbortSignal.timeout(15 * 60000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const type = (res.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
  if (/text\/html|json/.test(type)) throw new Error(`not a media file (${type})`);
  const len = Number(res.headers.get('content-length') || 0);
  if (len > ARCHIVE_MAX_BYTES) throw new Error('larger than 1.5 GB');
  const file = path.join(dir, `${base}.${extFor(type, url)}`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(file));
  const { size } = await fs.stat(file);
  if (size < 200) throw new Error('empty file');
  return { file, bytes: size };
}
async function ensureRelease() {
  try { await run('gh', ['release', 'view', RELEASE, '-R', REPO]); }
  catch {
    await run('gh', ['release', 'create', RELEASE, '-R', REPO, '--title', 'Archive of original files',
      '--notes', 'Full-resolution copies of the images and videos of every NFT in the Chromospace collection. Updated automatically every night; the list is in data/archive.json.']);
  }
}
async function archiveOne(item, sources, kind, entry, archiveFailures) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'cs-arch-'));
  const base = `${item.contract.slice(2, 10)}-${item.tokenId.slice(0, 40).replace(/[^0-9a-z]/gi, '')}-${kind}`;
  const reasons = [];
  try {
    for (const src of sources) {
      for (const url of candidates(src)) {
        try {
          const { file, bytes } = await saveOriginal(url, dir, base);
          await run('gh', ['release', 'upload', RELEASE, file, '--clobber', '-R', REPO], { timeout: 30 * 60000, maxBuffer: 1e7 });
          entry[kind] = path.basename(file);
          entry[kind + 'Bytes'] = bytes;
          entry[kind + 'Source'] = url;
          return true;
        } catch (e) { reasons.push(`${url.slice(0, 70)} → ${e.message}`); }
      }
    }
    archiveFailures.push(`${item.title} (${kind}): ${reasons.slice(0, 2).join(' | ') || 'no source'}`);
    return false;
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
}

async function readJSON(file, fallback) {
  try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch { return fallback; }
}

/* ---------- Main ---------- */
async function main() {
  const previous = await readJSON(path.join(DATA, 'nfts.json'), { items: [] });
  const artistsFile = await readJSON(path.join(DATA, 'artists.json'), {});
  const lowerKeys = o => Object.fromEntries(Object.entries(o || {}).map(([k, v]) => [lower(k), v]));
  const map = { tokens: lowerKeys(artistsFile.tokens), contracts: lowerKeys(artistsFile.contracts), collections: artistsFile.collections || {} };
  const exclude = await readJSON(path.join(DATA, 'exclude.json'), {});
  const exContracts = new Set((exclude.contracts || []).map(lower));
  const exCollections = new Set((exclude.collections || []).map(lower));
  const keepKeys = new Set((exclude.keep || []).map(lower));
  const archive = await readJSON(path.join(DATA, 'archive.json'), { items: {} });
  archive.release = RELEASE;
  archive.base = `https://github.com/${REPO}/releases/download/${RELEASE}/`;
  const prevByKey = new Map(previous.items.filter(i => i.key).map(i => [i.key, i]));

  // OpenSea: current images and collection slugs.
  const osNft = {};
  if (OS_KEY) {
    try {
      let next = '';
      for (let p = 0; p < 40; p++) {
        const j = await osGet(`/chain/ethereum/account/${WALLET}/nfts?limit=200${next ? '&next=' + encodeURIComponent(next) : ''}`);
        for (const n of (j && j.nfts) || []) {
          osNft[`${lower(n.contract)}:${n.identifier}`] = {
            display: n.display_image_url || n.image_url || '',
            full: n.original_image_url || fullSize(n.image_url || n.display_image_url || ''),
            animation: n.original_animation_url || n.display_animation_url || n.animation_url || '',
            slug: n.collection || '', name: n.name || '', url: n.opensea_url || ''
          };
        }
        if (!j || !j.next) break;
        next = j.next; await sleep(300);
      }
      console.log(`OpenSea: ${Object.keys(osNft).length} NFTs listed`);
    } catch (e) { console.warn('OpenSea not available:', e.message); }
  } else {
    console.log('No OPENSEA_API_KEY secret: skipping OpenSea');
  }

  console.log('Reading NFTs held by', WALLET);
  const held = await getAll('/nft');
  console.log(`  ${held.length} tokens`);

  const arrived = {};
  try {
    const transfers = await getAll('/token-transfers', { filter: 'to' });
    for (const t of transfers) {
      if (lower(t.to && (t.to.hash || t.to)) !== WALLET) continue;
      const ids = t.total && t.total.token_id != null ? [t.total.token_id] : (t.token_ids || []);
      for (const id of ids) {
        const k = `${tokenAddr(t.token)}:${id}`;
        if (!arrived[k] || t.timestamp > arrived[k]) arrived[k] = t.timestamp;
      }
    }
    console.log(`  ${transfers.length} incoming transfers`);
  } catch (e) { console.warn('  Could not read transfers, keeping previous dates:', e.message); }

  await fs.mkdir(THUMBS, { recursive: true });
  const usedIds = new Set(previous.items.map(i => i.id));
  let next = Math.max(0, ...[...usedIds].map(id => Number(String(id).replace(/\D/g, '')) || 0)) + 1;
  const items = [], thumbJobs = [], sourcesByKey = {};
  let skipped = 0;

  for (const n of held) {
    const t = n.token || {};
    const md = n.metadata || {};
    const contract = tokenAddr(t);
    const collection = t.name || '';
    const tokenId = String(n.id);
    const key = `${contract}:${tokenId}`;
    if (!keepKeys.has(key) && (exContracts.has(contract) || exCollections.has(lower(collection)))) { skipped++; continue; }
    const prev = prevByKey.get(key);
    const osn = osNft[key] || {};
    const ts = arrived[key];
    const metaImages = [
      n.thumbnails && (n.thumbnails['500x500'] || n.thumbnails['original']),
      n.image_url, md.image, md.image_url,
      md.image_data && `data:image/svg+xml;utf8,${encodeURIComponent(md.image_data)}`
    ];
    const thumbSources = [...new Set([osn.display, ...metaImages, n.animation_url, md.animation_url].filter(x => typeof x === 'string' && x))];
    sourcesByKey[key] = {
      image: [...new Set([osn.full, ...metaImages.filter(x => x && !String(x).includes('thumbnails')), osn.display].filter(x => typeof x === 'string' && x))],
      animation: [...new Set([osn.animation, n.animation_url, md.animation_url].filter(x => typeof x === 'string' && x && !/\.html?($|\?)/i.test(x)))]
    };

    let id = prev && prev.id;
    if (!id) { while (usedIds.has(`CS-N${String(next).padStart(3, '0')}`)) next++; id = `CS-N${String(next).padStart(3, '0')}`; usedIds.add(id); next++; }

    const item = {
      id, key, category: 'nft',
      title: md.name || osn.name || `${collection || 'Token'} #${tokenId}`,
      artist: artistFor(md, collection, contract, key, map),
      collection, contract, tokenId,
      standard: n.token_type || t.type || '',
      copies: Number(n.value) > 1 ? Number(n.value) : undefined,
      acquired: ts ? ts.slice(0, 10) : (prev && prev.acquired) || null,
      thumb: '', thumbSource: '',
      image: osn.display || publicUrl(n.image_url || md.image || md.image_url || ''),
      animation: publicUrl(n.animation_url || md.animation_url || ''),
      opensea: osn.url || '',
      slug: osn.slug || '',
      note: String(md.description || '').slice(0, 600)
    };
    items.push(item);

    const thumbName = `${contract.slice(2, 10)}-${tokenId.slice(0, 40).replace(/[^0-9a-z]/gi, '')}.webp`;
    const thumbFile = path.join(THUMBS, thumbName);
    let exists = false;
    try { await fs.access(thumbFile); exists = true; } catch {}
    // Remake the thumbnail when a better source (OpenSea's current image) is available.
    const preferred = thumbSources[0] || '';
    if (exists && prev && prev.thumbSource && (prev.thumbSource === preferred || !osn.display)) {
      item.thumb = `thumbs/${thumbName}`; item.thumbSource = prev.thumbSource;
    } else if (thumbSources.length) {
      if (exists) { item.thumb = `thumbs/${thumbName}`; item.thumbSource = (prev && prev.thumbSource) || ''; }
      thumbJobs.push(async () => {
        const tmp = thumbFile + '.tmp.webp';
        const used = await makeThumb(thumbSources, tmp, item.title);
        if (used) { await fs.rename(tmp, thumbFile); item.thumb = `thumbs/${thumbName}`; item.thumbSource = used; }
        else await fs.rm(tmp, { force: true });
      });
    }
  }
  console.log(`  ${skipped} excluded, ${items.length} kept, ${thumbJobs.length} thumbnails to make`);
  if (!items.length && previous.items.length) throw new Error('Blockscout returned no tokens; refusing to wipe existing data');

  await pool(thumbJobs.slice(0, MAX_NEW_THUMBS), PARALLEL);

  items.sort((a, b) => (b.acquired || '').localeCompare(a.acquired || '') || a.id.localeCompare(b.id));
  const keep = new Set(items.map(i => i.thumb).filter(Boolean).map(t => path.basename(t)));
  for (const f of await fs.readdir(THUMBS)) {
    if (f.endsWith('.webp') && !keep.has(f)) await fs.unlink(path.join(THUMBS, f));
  }

  const save = async () => {
    await fs.writeFile(path.join(DATA, 'nfts.json'), JSON.stringify({
      _readme: 'Generated every day by scripts/update-nfts.mjs. Do not edit by hand: change data/artists.json or data/exclude.json instead.',
      source: 'Blockscout (eth.blockscout.com)' + (OS_KEY ? ' + OpenSea' : ''),
      updated: new Date().toISOString(),
      wallet: WALLET,
      items
    }, null, 1) + '\n');
    archive._readme = 'Full-resolution originals, kept as assets of the GitHub release "archive". Download link = base + file name. Written automatically.';
    archive.updated = new Date().toISOString();
    await fs.writeFile(path.join(DATA, 'archive.json'), JSON.stringify(archive, null, 1) + '\n');
  };
  await save();
  console.log(`Saved ${items.length} NFTs, ${items.filter(i => i.thumb).length} with a thumbnail.`);
  if (thumbFailures.length) {
    console.log(`\n${thumbFailures.length} thumbnails could not be made:`);
    thumbFailures.slice(0, 60).forEach(f => console.log('  - ' + f));
  }

  // Who made what, according to OpenSea (for checking data/artists.json).
  if (OS_KEY) {
    try {
      const report = {}, accounts = {};
      const username = async addr => {
        if (!addr) return '';
        if (!(addr in accounts)) { const a = await osGet(`/accounts/${addr}`).catch(() => null); accounts[addr] = (a && a.username) || ''; await sleep(250); }
        return accounts[addr];
      };
      const byContract = {};
      items.forEach(i => (byContract[i.contract] = byContract[i.contract] || []).push(i));
      for (const [contract, list] of Object.entries(byContract)) {
        const slug = list.find(i => i.slug)?.slug || '';
        const col = slug ? await osGet(`/collections/${slug}`).catch(() => null) : null;
        await sleep(250);
        const entry = {
          collection: list[0].collection, opensea_collection: (col && col.name) || '', slug,
          owner: (col && col.owner) || '', owner_username: await username(col && col.owner),
          artist_on_site: [...new Set(list.map(i => i.artist))].join(' / '),
          works: list.length, examples: list.slice(0, 3).map(i => i.title)
        };
        if (SHARED.has(contract)) {
          entry.creators = {};
          for (const i of list) {
            const d = await osGet(`/chain/ethereum/contract/${contract}/nfts/${i.tokenId}`).catch(() => null);
            const c = d && d.nft && d.nft.creator;
            entry.creators[i.title] = c ? (await username(c)) || c : '';
            await sleep(250);
          }
        }
        report[contract] = entry;
      }
      await fs.writeFile(path.join(DATA, 'artists-report.json'), JSON.stringify(report, null, 1) + '\n');
      console.log(`Artist report written for ${Object.keys(report).length} collections.`);
    } catch (e) { console.warn('Artist report failed:', e.message); }
  }

  // Archive of originals.
  if (ARCHIVE_ON) {
    try { await ensureRelease(); } catch (e) { console.warn('Archive skipped, release not available:', e.message); return; }
    const archiveFailures = [];
    let done = 0;
    for (const item of items) {
      if ((Date.now() - started) / 60000 > ARCHIVE_MINUTES) { console.log('Archive time budget used; continuing next run.'); break; }
      const entry = archive.items[item.key] || (archive.items[item.key] = { title: item.title });
      entry.title = item.title;
      const src = sourcesByKey[item.key];
      if (!entry.image && src.image.length) { if (await archiveOne(item, src.image, 'image', entry, archiveFailures)) done++; }
      if (!entry.animation && src.animation.length) { if (await archiveOne(item, src.animation, 'animation', entry, archiveFailures)) done++; }
      if (done && done % 20 === 0) await save();
    }
    // Mark entries of NFTs that left the wallet (their files stay in the release).
    const current = new Set(items.map(i => i.key));
    for (const k of Object.keys(archive.items)) if (!current.has(k)) archive.items[k].left_wallet = true;
    await save();
    const total = Object.values(archive.items).filter(e => e.image).length;
    console.log(`Archive: ${done} new files, ${total} NFTs with an original image saved.`);
    if (archiveFailures.length) {
      console.log(`\n${archiveFailures.length} originals could not be saved:`);
      archiveFailures.slice(0, 40).forEach(f => console.log('  - ' + f));
    }
  }
}

main().catch(e => { console.error('Update failed:', e.message); process.exit(1); });
