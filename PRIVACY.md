# Privacy policy

FastSync is a free, open-source desktop app for Linux that syncs folders with Dropbox, Google Drive, OneDrive and MEGA.
Source: <https://github.com/evandro777/FastSync>

## What FastSync does with your data

- FastSync runs **only on your computer**. It has no server, no account system, no analytics and no telemetry.
- It talks directly to the cloud service you log in to (through [rclone](https://rclone.org)), to list, upload,
  download and delete the files in the folders you chose to sync. Nothing is sent anywhere else.
- Your login tokens, and the client ID and secret if you use your own, are stored only in FastSync's `rclone.conf`
  on your computer (permissions `0600`).
- The developer does not receive, collect, store, sell or share your files, your tokens or any personal data.

## Google user data

When you log in with Google, FastSync asks for the `https://www.googleapis.com/auth/drive` scope so it can create,
edit and delete files in the Drive folder you chose. That access is used only to sync that folder with your
computer. The data stays between your computer and Google; it is not transferred to the developer or to any third
party, and is not used for advertising or to train any model.

## Other services

- **Dropbox:** FastSync asks for `account_info.read`, `files.metadata.write`, `files.content.read`,
  `files.content.write` and `sharing.write` (to create a share link when you ask for one).
- **OneDrive:** it asks for read and write access to your files (`Files.ReadWrite.All`), your basic profile
  (`User.Read`) and `offline_access`, so it can keep syncing without asking you to log in again.
- **MEGA:** you give FastSync your email and password. The password is stored obscured (not encrypted) in the same
  `rclone.conf`.
- The same rules apply: the data goes only between your computer and the service, never to the developer.

## Notifications and links

FastSync shows desktop notifications (for example when two copies of a file were kept) and, when you choose "Copy
share link" in your file manager, asks the service for a public link to that one file and shows it in a window, where a button copies it. Nothing is
shared unless you ask for it.

## Removing access

Remove the account in FastSync, or revoke it in your Google account under *Manage third-party access* (Dropbox:
*Connected apps*; Microsoft: *Apps and services*). Deleting FastSync's data folder removes the stored tokens.

## Contact

Questions: open an issue at <https://github.com/evandro777/FastSync/issues>.
