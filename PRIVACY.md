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

## Removing access

Remove the account in FastSync, or revoke it in your Google account under *Manage third-party access* (Dropbox:
*Connected apps*; Microsoft: *Apps and services*). Deleting FastSync's data folder removes the stored tokens.

## Contact

Questions: open an issue at <https://github.com/evandro777/FastSync/issues>.
