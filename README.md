# Chromospace

Website of the Chromospace art collection: street art, prints, paintings, trading cards and NFTs.
Live at https://chromospace.art (GitHub Pages, branch `main`, root folder).

## How it works

- `index.html` is the whole site: Home (piece of the day, selected NFTs, artists, latest acquisitions), Artists, one page per artist, Collection.
- The site reads its content from `data/`:
  - `data/physical.json`: physical works. Edit by hand. Entries with `"example": true` are placeholders.
  - `data/nfts.json`: NFTs in wallet `0xb884ad7ab6af74693fec7f364eb8bfb31fc7ecea`. **Written automatically, do not edit.**
  - `data/featured.json`: titles of the NFTs shown on the home page. Empty list = automatic pick.
  - `data/artists.json`: collection name → artist name, for NFTs whose metadata doesn't name the artist.
- `thumbs/`: small .webp images of the NFTs, made automatically so pages load fast.

## Daily update

`.github/workflows/update-nfts.yml` runs every night (and from the Actions tab → "Daily NFT update" → "Run workflow").
It runs `scripts/update-nfts.mjs`, which reads the wallet from the public Blockscout API, records when each token arrived,
makes thumbnails for new tokens and commits the changes. If Blockscout is down, nothing is changed.
