# Cloudflare R2 Release Mirror

Ratash publishes macOS installers to the `ratash-releases` R2 bucket after the GitHub Release succeeds. The public custom domain is `ratash.zoubingwu.com`; its `r2.dev` URL remains disabled.

The release workflow stores immutable assets under `releases/vVERSION/`, uploads the bootstrap installer at `install.sh`, and updates `releases/latest.json` last. The publisher accepts an existing versioned object only when its content is identical and rejects conflicting content. This ordering keeps the public latest pointer on a complete release.

## Cloudflare resources

- Account ID: `49f76aa38e951698872cb77926d235e2`
- Zone ID: `380af114cdcec793ab7ab43dff46afb1`
- Bucket: `ratash-releases`, Standard storage, APAC location hint
- Custom domain: `ratash.zoubingwu.com`, minimum TLS 1.2

The zone cache rule for versioned assets matches:

```text
http.host eq "ratash.zoubingwu.com" and starts_with(http.request.uri.path, "/releases/v")
```

It enables cache eligibility and sets a one-year edge TTL. Published versioned objects also carry `Cache-Control: public, max-age=31536000, immutable`. The latest manifest uses 60 seconds, and the bootstrap installer uses 300 seconds.

## GitHub Actions credential

Create a Cloudflare API token scoped to Account / Workers R2 Storage / Edit for the Ratash account. Store it as the repository Actions secret `CLOUDFLARE_API_TOKEN`. The workflow supplies the non-secret account ID directly.

To publish an already-built distribution locally:

```sh
CLOUDFLARE_ACCOUNT_ID=49f76aa38e951698872cb77926d235e2 \
CLOUDFLARE_API_TOKEN=... \
scripts/publish-cloudflare-r2.sh 0.1.2 dist
```
