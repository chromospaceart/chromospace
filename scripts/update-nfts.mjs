// Daily update of data/nfts.json from the public Blockscout API.
// Runs in GitHub Actions (see .github/workflows/update-nfts.yml). Node 20+, needs `sharp`.
// - Reads every ERC-721 / ERC-1155 token held by the wallet.
// - Finds the date each token arrived in the wallet.
// - Makes a small .webp thumbnail for each new token in thumbs/, so the site loads fast.
// If Blockscout cannot be reached, the script exits without touching existing data.

import fs from 'node:fs/promises';
import path from 'node:path';
import sharp from 'sharp';

const WALLET = '0xb884ad7ab6af74693fec7f364eb8bfb31fc7ecea';
const API = `https://eth.blockscout.com/api/v2/addresses/${WALLET}`;
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const DATA = path.join(ROOT, 'data');
const THUMBS = path.join(ROOT, 'thumbs');
const MAX_PAGES = 60;            // 50 items per page
const MAX_NEW_THUMBS = 200;      // per run, keeps the job short; the rest follow the next day
const THUMB_WIDTH = 640;
const GATEWAYS = ['https://ipfs.io/ipfs/', 'https://dweb.link/ipfs/', 'https://nftstorage.link/ipfs/'];

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getJSON(url, tries = 3) {
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(30000) });
      if (res.status === 429) { await sleep(3000 * (i + 1)); continue; }
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return await res.json();
    } catch (e) {
      if (i === tries - 1) throw e;
      await sleep(2000 * (i + 1));
    }
  }
  throw new Error(`Gave up on ${url}`);
}

async function getAll(endpoint, extra = {}) {
  const base = { type: 'ERC-721,ERC-1155', ...extra };
  let params = new URLSearchParams(base);
  const out = [];
  for (let p = 0; p < MAX_PAGES; p++) {
    const j = await getJSON(`${API}${endpoint}?${params}`);
    out.push(...(j.items || []));
    if (!j.next_page_params) break;
    params = new URLSearchParams({ ...base, ...j.next_page_params });
    await sleep(250);
  }
  return out;
}

const lower = s => String(s || '').toLowerCase();
const tokenAddr = t => lower(t && (t.address_hash || t.address || t.hash));

function ipfsPath(u) {
  if (u.startsWith('ipfs://')) return u.slice(7).replace(/^ipfs\//, '');
  const m = u.match(/\/ipfs\/(.+)$/);
  return m ? m[1] : null;
}
function candidates(u) {
  if (!u || typeof u !== 'string') return [];
  if (u.startsWith('data:')) return [u];
  if (u.startsWith('ar://')) return ['https://arweave.net/' + u.slice(5)];
  const p = ipfsPath(u);
  if (p) return GATEWAYS.map(g => g + p);
  return [u];
}
function publicUrl(u) {
  const url = candidates(u)[0] || '';
  return url.startsWith('data:') && url.length > 4000 ? '' : url; // keep the JSON small
}

function artistFor(md, collection, map) {
  const fromMd = md.artist || md.created_by || md.creator || md.artist_name ||
    (Array.isArray(md.attributes) && (md.attributes.find(a => /^(artist|creator|created by)$/i.test(a.trait_type || ''))?.value));
  if (fromMd && typeof fromMd === 'string' && !/^0x[0-9a-f]{40}$/i.test(fromMd)) return fromMd.trim();
  if (map[collection]) return map[collection];
  const by = collection.match(/\bby\s+(.+)$/i);
  if (by) return by[1].trim();
  return collection || 'Unattributed';
}

async function readBytes(url) {
  if (url.startsWith('data:')) {
    const comma = url.indexOf(',');
    const meta = url.slice(5, comma);
    const body = url.slice(comma + 1);
    return meta.includes(';base64') ? Buffer.from(body, 'base64') : Buffer.from(decodeURIComponent(body));
  }
  const res = await fetch(url, { signal: AbortSignal.timeout(25000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const type = res.headers.get('content-type') || '';
  if (/video|html|json/.test(type)) throw new Error(`not an image: ${type}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length > 40 * 1024 * 1024) throw new Error('image too large');
  return buf;
}

async function makeThumb(sources, file) {
  for (const src of sources) {
    for (const url of candidates(src)) {
      try {
        const buf = await readBytes(url);
        await sharp(buf, { animated: false, limitInputPixels: 2e8 })
          .rotate()
          .resize({ width: THUMB_WIDTH, height: THUMB_WIDTH * 1.25, fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 78 })
          .toFile(file);
        return true;
      } catch (e) { /* try the next source */ }
    }
  }
  return false;
}

async function readJSON(file, fallback) {
  try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch { return fallback; }
}

async function main() {
  const previous = await readJSON(path.join(DATA, 'nfts.json'), { items: [] });
  const artistsMap = (await readJSON(path.join(DATA, 'artists.json'), {})).collections || {};
  const prevByKey = new Map(previous.items.filter(i => i.key).map(i => [i.key, i]));

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
  } catch (e) {
    console.warn('  Could not read transfers, keeping previous dates:', e.message);
  }

  await fs.mkdir(THUMBS, { recursive: true });
  let usedIds = new Set(previous.items.map(i => i.id));
  let next = Math.max(0, ...[...usedIds].map(id => Number(String(id).replace(/\D/g, '')) || 0)) + 1;
  let newThumbs = 0;
  const items = [];

  for (const n of held) {
    const t = n.token || {};
    const md = n.metadata || {};
    const contract = tokenAddr(t);
    const tokenId = String(n.id);
    const key = `${contract}:${tokenId}`;
    const prev = prevByKey.get(key);
    const collection = t.name || '';
    const ts = arrived[key];
    const imageSources = [
      n.thumbnails && (n.thumbnails['500x500'] || n.thumbnails['original']),
      n.image_url, md.image, md.image_url, md.image_data && `data:image/svg+xml;utf8,${encodeURIComponent(md.image_data)}`
    ].filter(Boolean);

    const thumbName = `${contract.slice(2, 10)}-${tokenId.slice(0, 40).replace(/[^0-9a-z]/gi, '')}.webp`;
    const thumbFile = path.join(THUMBS, thumbName);
    let thumb = '';
    try { await fs.access(thumbFile); thumb = `thumbs/${thumbName}`; } catch {}
    if (!thumb && newThumbs < MAX_NEW_THUMBS && imageSources.length) {
      newThumbs++;
      if (await makeThumb(imageSources, thumbFile)) thumb = `thumbs/${thumbName}`;
    }

    let id = prev && prev.id;
    if (!id) { while (usedIds.has(`CS-N${String(next).padStart(3, '0')}`)) next++; id = `CS-N${String(next).padStart(3, '0')}`; usedIds.add(id); next++; }

    items.push({
      id, key, category: 'nft',
      title: md.name || `${collection || 'Token'} #${tokenId}`,
      artist: artistFor(md, collection, artistsMap),
      collection, contract, tokenId,
      standard: n.token_type || t.type || '',
      copies: Number(n.value) > 1 ? Number(n.value) : undefined,
      acquired: ts ? ts.slice(0, 10) : (prev && prev.acquired) || null,
      thumb,
      image: publicUrl(imageSources[0] || ''),
      animation: publicUrl(n.animation_url || md.animation_url || ''),
      note: String(md.description || '').slice(0, 600)
    });
  }

  if (!items.length && previous.items.length) {
    throw new Error('Blockscout returned no tokens; refusing to wipe existing data');
  }

  items.sort((a, b) => (b.acquired || '').localeCompare(a.acquired || '') || a.id.localeCompare(b.id));

  // Remove thumbnails of tokens that left the wallet.
  const keep = new Set(items.map(i => i.thumb).filter(Boolean).map(t => path.basename(t)));
  for (const f of await fs.readdir(THUMBS)) {
    if (f.endsWith('.webp') && !keep.has(f)) await fs.unlink(path.join(THUMBS, f));
  }

  const out = {
    _readme: previous._readme || 'Generated every day by scripts/update-nfts.mjs. Do not edit by hand.',
    source: 'Blockscout (eth.blockscout.com)',
    updated: new Date().toISOString(),
    wallet: WALLET,
    items
  };
  await fs.writeFile(path.join(DATA, 'nfts.json'), JSON.stringify(out, null, 1) + '\n');
  console.log(`Saved ${items.length} NFTs, ${newThumbs} new thumbnails attempted.`);
}

main().catch(e => { console.error('Update failed:', e.message); process.exit(1); });
