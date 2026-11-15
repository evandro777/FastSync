# Use your own client ID

FastSync logs in to Dropbox, Google Drive and OneDrive through [rclone](https://rclone.org), which by default uses
a **client ID shared by every rclone user**. The services limit how many requests that one shared ID may make, so
sometimes syncing stops for a while with an error such as `rateLimitExceeded` / `Quota exceeded for quota metric`.
Google has also said rclone's shared Drive client ID will stop working during 2026.

The fix is a **client ID of your own**: a small "app" you register for free with the service. It has its own
quota, so nobody else's traffic counts against you. You do this once per service, then paste two values into
FastSync's **Add account** dialog. For an account you already have, use the pencil button on its card, tick
"Use my own client ID", fill both fields and press **Next**: the browser opens to sign in again.

- Cost: free (OneDrive's Azure sign-up asks for an address and a card to verify your identity; it is not charged unless you upgrade to a paid plan). OneDrive is optional: without your own ID FastSync uses rclone's shared one.
- Time: about 10 minutes.
- Safe: the values only let *your* app ask *your* accounts for access. FastSync keeps them in its own `rclone.conf` (permissions `0600`) and never sends them anywhere except to the service you log in to.

## In FastSync

1. **Add account**, pick the service and the folders.
2. Tick **Use my own client ID (advanced)**.
3. Paste the **Client ID** and **Client secret** (called *App key* / *App secret* by Dropbox, *Application (client) ID* / secret *Value* by Microsoft).
4. **Log in** and finish in the browser as usual.

Both values are needed, without spaces. Already added an account with the shared ID? Remove it and add it again
with your own ID (your files stay where they are).

---

## Google Drive

Google's pages and button names change from time to time; the steps below are the current ones from rclone's
documentation.

1. Open the [Google API Console](https://console.cloud.google.com/) and sign in. Any Google account works; it need not be the one whose Drive you will sync.
2. Select a project, or **create a new project**.
3. Under **Enable APIs and services**, search for **Google Drive API** and enable it.
4. In the left panel click **Credentials** (not "Create credentials").
5. If you have no **OAuth consent screen** yet: **Configure consent screen**, **Get started**. Application name (anything, for example `FastSync`), a support email (yours is fine). Audience: **External** (choose **Internal** only if you use Google Workspace and the Drive is in the same organisation). Add your contact email, accept the terms, **Create**.
6. Open **Data Access**, **Add or remove scopes**, and add `https://www.googleapis.com/auth/drive` (needed to create, edit and delete files). **Add to table**, **Update**, then **Save**.
7. Open **Audience**, **+ Add users**, add your own Google account as a test user, **Save**.
8. Go to **Overview**, **Create OAuth client**, application type **Desktop app**, **Create**.
9. Copy the **Client ID** and **Client secret**.
10. **Publish the app** (External only). This matters: an app left in *Testing* makes your login **expire after 7 days**, so FastSync would ask you to log in again every week.
    1. **Branding**: fill in *App name*, *Application home page*, *Application privacy policy link* and, under *Authorized domains*, the domain of those links, then **Save**. Google requires them even for a personal app. FastSync's own pages work: `https://github.com/evandro777/FastSync` and `https://github.com/evandro777/FastSync/blob/master/PRIVACY.md`, with the domain `github.com`.
    2. Do **not** add a *logo*: it forces Google's verification process.
    3. Do **not** click *Verify branding* or *Go to verification center*: Google refuses a `github.com` page ("not registered to you") and you do not need it.
    4. **Audience**, **Publish app**, confirm. The status must read **In production**. A yellow "Your app requires verification" banner stays; ignore it. Do not press *Back to testing*.
11. In FastSync, paste the two values and log in. Google shows a scary **"Google hasn't verified this app"** screen. That is expected for your own personal app: **Advanced**, **Go to ... (unsafe)**, then allow.

Verification by Google is only needed to remove that warning and to go past 100 users; for yourself you can skip it.

If creating the consent screen fails with *"changes to one of the field of the resource is not supported"*, use
Google's Python quickstart page and press **Enable the Drive API**: it creates a project, a client ID and a
secret for you.

---

## Dropbox

1. Open the [Dropbox App Console](https://www.dropbox.com/developers/apps) and sign in (any Dropbox account).
2. **Create app**. Choose the API **Scoped access**.
3. Type of access: **Full Dropbox** (choose it if you want to sync team folders or any folder; *App folder* limits the app to one folder named after it).
4. Name the app. The name must be unique across Dropbox (for example `FastSync-yourname`). **Create app**.
5. **Permissions** tab: enable `account_info.read`, `files.metadata.write`, `files.content.write`, `files.content.read` and `sharing.write` (the two `*.read` metadata/sharing boxes get ticked by themselves). Click **Submit**.
6. **Settings** tab, *OAuth 2, Redirect URIs*: type `http://localhost:53682/` and click **Add**. (Type it; the address must match exactly.)
7. On the same tab, look for "Credentials", copy the **App key** (this is the *Client ID*) and **App secret** (the *Client secret*).
8. In FastSync, paste them and log in.

A Dropbox app starts in *Development* status, and on the **Settings** tab *Development users* reads **Only you**: only the Dropbox account that created the app can log in. Any other account fails with "This app has reached its user limit". To use a second account, click **Enable additional users** on that tab (up to 500 users, plenty for yourself); no production review is needed.

---

## OneDrive

1. Open the [Azure portal](https://portal.azure.com/) and sign in with a Microsoft account. First time: Azure asks you to create a free account and may ask for a phone number and a card for identity verification (use a temporary one). This step also creates the *directory* (tenant) that owns your app. Without one, registering fails with "The ability to create applications outside of a directory has been deprecated": finish the [free Azure sign-up](https://azure.microsoft.com/free) first (you do not have to use any paid service), then reload the portal.
2. Search for **App registrations**, **New registration**.
    1. Direct link: <https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade>
3. Name: anything. Supported account types: **Any Entra ID Tenant + Personal Microsoft accounts** (older portals call it *Accounts in any organizational directory (Any Azure AD directory - Multitenant) and personal Microsoft accounts*). Redirect URI: platform **Web**, value `http://localhost:53682/` (type it, do not paste it). **Register**.
4. Copy the **Application (client) ID** shown under the app name: this is the *Client ID*.
5. On the app's *Overview* click **Add a certificate or secret** (right side, under *Client credentials*; also in the left menu, **Manage**, **Certificates & secrets**). **New client secret**, any description, the longest expiry offered (24 months), **Add**. Copy the secret's **Value** now (not the *Secret ID*): Azure shows it only once. This is the *Client secret*.
6. **API permissions**, **Add a permission**, **Microsoft Graph**, **Delegated permissions**. Select `Files.Read`, `Files.ReadWrite`, `Files.Read.All`, `Files.ReadWrite.All`, `offline_access`, `User.Read` and `Sites.Read.All`, then **Add permissions**.
7. In FastSync, paste the two values and log in.

Notes:

- The orange banner "End users cannot grant consent to newly registered multitenant apps without verified publishers" can be ignored for a personal Microsoft account: it only matters for work or school accounts that need an administrator's approval.
- **The secret expires** after the period you chose. Then logins and token refreshes fail: create a new secret in the same place and re-add the account.
- **OneDrive for work or school** may refuse the steps above (a common error is "publisher not verified"), or your organisation may restrict apps. Create the app with your work account and pick **Accounts in this organizational directory only (Single tenant)** instead; FastSync does not set the tenant-specific login addresses for you yet, so work/school accounts may still need your IT department or the rclone documentation.

---

## Problems

| Message | Likely cause |
| --- | --- |
| *redirect_uri_mismatch* or "redirect URI" error (Dropbox, OneDrive) | The redirect address is not exactly `http://localhost:53682/`. Edit it in the app's settings. |
| *access_denied* / "app not verified" and you cannot continue (Google) | Your account is not a test user (step 7) or the app is not published and you chose a different account. |
| *invalid_client* | The ID or secret was pasted wrongly (extra space or a cut-off character), or for Microsoft you copied the *Secret ID* instead of the *Value*. |
| Login worked, but fails again after a week (Google) | The app is still in *Testing*: publish it (step 10). |
| Port 53682 already in use | Another rclone login is running or waiting. Close it, or wait a minute and try again. |

## Remove or rotate

- Delete the app in the service's console to revoke everything it was granted.
- Changed the secret? Remove the account in FastSync and add it again with the new values. Your files are not touched.
- Dropbox: also see *Connected apps* in your Dropbox security settings. Google: *Manage third-party access* in your Google account. Microsoft: *Apps and services* in your Microsoft account privacy page.
