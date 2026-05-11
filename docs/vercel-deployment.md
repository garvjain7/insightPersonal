# Deploy DataInsights.ai on Vercel

This repository is a monorepo: a **Vite + React** app in `frontend-react` and a **Node (Express) API** in `backend-node`. Vercel is configured to build and host the **static frontend** from the repo root. The API is not run on Vercel in this setup (it needs long-lived processes, Redis, optional workers, and local file storage). Deploy the API on a container or Node host (Railway, Render, Fly.io, your own VM, etc.) and point the frontend at it with an environment variable.

---

## What you get after following this guide

- A production URL for the React app (for example `https://your-project.vercel.app`).
- The app calls your backend using `VITE_API_BASE_URL` (must be the public URL of your API, including `/api`).

---

## Prerequisites

- A [Vercel](https://vercel.com) account.
- The repository pushed to GitHub, GitLab, or Bitbucket (Vercel imports from Git).
- A **running backend** with a public HTTPS URL (see [Backend (separate host)](#backend-separate-host)).

---

## Step 1: Import the project in Vercel

1. Log in to [vercel.com](https://vercel.com) and open the dashboard.
2. Click **Add New…** → **Project**.
3. **Import** your `DataInsights.ai` Git repository.
4. On the **Configure Project** screen, leave the default **Framework Preset** as detected or choose **Other** if prompted. The repo includes a root `vercel.json` that overrides install, build, and output directory, so you do not need to set **Root Directory** to `frontend-react` unless you prefer that workflow (see [Alternative: Root Directory](#alternative-root-directory-only-frontend-react)).

---

## Step 2: Configure environment variables

The frontend reads the API base URL at **build time** (Vite embeds `VITE_*` variables into the bundle).

1. In the Vercel project, open **Settings** → **Environment Variables**.
2. Add:

   | Name | Value | Environments |
   |------|--------|----------------|
   | `VITE_API_BASE_URL` | Your API base URL, **must** end with `/api` (same path the Express app uses under `/api`) | Production, Preview, Development (as needed) |

   Example values:

   - `https://api.yourdomain.com/api`
   - `https://your-app.onrender.com/api`

3. Save. **Redeploy** after changing this variable so a new build picks up the value.

**Note:** `VITE_API_BASE_URL` must use **HTTPS** in production so browsers do not block mixed content when the site is served from Vercel.

---

## Step 3: Deploy

1. Click **Deploy** (first import) or push a commit to the connected branch to trigger a deployment.
2. Wait for **Build** to finish. The build runs `npm install` and `npm run build` inside `frontend-react`, then publishes `frontend-react/dist`.
3. Open the deployment URL and test login and API-backed pages.

---

## Step 4: Custom domain (optional)

1. In the project: **Settings** → **Domains**.
2. Add your domain and follow Vercel’s DNS instructions.
3. If your API is on another subdomain (for example `api.example.com`), set `VITE_API_BASE_URL` to `https://api.example.com/api` and redeploy.

---

## Backend (separate host)

The Express app in `backend-node` expects environment variables (database, Redis, JWT secrets, etc.) as in your local or Docker setup. Typical steps:

1. Deploy `backend-node` to your chosen host with **Node 18+**.
2. Set `PORT` to the value your host provides (or their default).
3. Ensure **CORS** allows your Vercel origin. Today the server uses `cors()` with default options (reflects request origin in many cases). For stricter production rules, restrict origins to your Vercel URL and custom domain.
4. Point the frontend at the API using `VITE_API_BASE_URL` as in Step 2.

The ML pipeline under `ml_engine` is separate; run it where you already run workers or batch jobs (not required for the static Vercel frontend to load).

---

## How the Vercel config works

Root `vercel.json`:

- **`installCommand`** / **`buildCommand`**: Install and build only `frontend-react` (no need to change **Root Directory** in the dashboard).
- **`outputDirectory`**: Publishes the Vite `dist` folder.
- **`framework`**: `null` avoids wrong auto-detection for a nested app.
- **`rewrites`**: Sends unknown paths to `index.html` so **React Router** deep links (for example `/login`, `/dashboard`) work on refresh and direct opens.

---

## Alternative: Root Directory = `frontend-react`

If you prefer the Vercel **Root Directory** set to `frontend-react`:

1. Set **Root Directory** to `frontend-react` in **Settings** → **General**.
2. Remove or ignore the root `vercel.json` and add a minimal `vercel.json` inside `frontend-react` with only the SPA rewrite:

   ```json
   {
     "rewrites": [{ "source": "/(.*)", "destination": "/index.html" }]
   }
   ```

3. Use default **Build Command** `npm run build` and **Output Directory** `dist`.
4. Still set `VITE_API_BASE_URL` in Vercel environment variables.

---

## Troubleshooting

| Symptom | What to check |
|--------|----------------|
| Blank API calls or wrong host | `VITE_API_BASE_URL` in Vercel, spelling, trailing `/api`, then **Redeploy**. |
| CORS errors in the browser | Backend allows your Vercel URL; API is HTTPS. |
| 404 on refresh for routes like `/login` | SPA rewrites: ensure `vercel.json` rewrites are present and you redeployed after adding them. |
| Build fails on Vercel | Build logs: run `npm run build` locally in `frontend-react` and fix errors; align Node version in **Settings** → **General** if needed. |

---

## Local check before deploying

From the repository root:

```powershell
npm install --prefix frontend-react
npm run build --prefix frontend-react
```

The output should appear under `frontend-react/dist`. This matches what Vercel runs via `vercel.json`.
