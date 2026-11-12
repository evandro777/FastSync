#!/usr/bin/env python3
"""Writes crates/app/translations/<lang>/LC_MESSAGES/fastsync.po (bundled by build.rs). Edit the table, run, commit.
Slint's default gettext context is the component name, so msgctxt is `Main`, `AccountCard`, `AddDialog`, `SettingsDialog`,
`About`, `AboutCard` (UI) or `Tr` (strings built in Rust)."""
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "crates", "app", "translations")
LANGS = ["pt_BR"]
# (context, msgid, [translation per LANGS])
T = [
    # Built in Rust (tray menu and messages).
    ("Tr", "Open FastSync", ["Abrir o FastSync"]),
    ("Tr", "Sync now", ["Sincronizar agora"]),
    ("Tr", "Pause syncing", ["Pausar a sincronização"]),
    ("Tr", "Resume syncing", ["Retomar a sincronização"]),
    ("Tr", "Quit", ["Sair"]),
    ("Tr", "Everything is up to date", ["Tudo está atualizado"]),
    ("Tr", "Syncing…", ["Sincronizando…"]),
    ("Tr", "Paused", ["Pausado"]),
    ("Tr", "Needs attention", ["Precisa de atenção"]),
    ("Tr", "No accounts", ["Nenhuma conta"]),
    ("Tr", "Choose a local folder, for example /home/you/Dropbox.", ["Escolha uma pasta local, por exemplo /home/voce/Dropbox."]),
    ("Tr", "Another account already syncs that folder.", ["Outra conta já sincroniza essa pasta."]),
    ("Tr", "Choose a different folder: this one holds your whole home folder or FastSync's own data.",
     ["Escolha outra pasta: esta contém toda a sua pasta pessoal ou os dados do próprio FastSync."]),
    ("Tr", "Fill in both the client ID and the client secret, without spaces, or turn the option off.",
     ["Preencha o ID e o segredo do cliente, sem espaços, ou desative a opção."]),
    ("Tr", "Your rclone was built without support for this service (Debian and Ubuntu's rclone leaves some out). Install the official rclone from rclone.org, or use the Flatpak.",
     ["O seu rclone foi compilado sem suporte a este serviço (o rclone do Debian e do Ubuntu deixa alguns de fora). Instale o rclone oficial em rclone.org ou use o Flatpak."]),
    ("Tr", "Open folder", ["Abrir pasta"]),
    ("Tr", "Login failed: {}", ["Falha no login: {}"]),
    # The accounts page.
    ("Main", "Add account", ["Adicionar conta"]),
    ("Main", "Preferences", ["Preferências"]),
    ("Main", "rclone was not found. FastSync needs it to talk to the cloud. Install it (rclone.org) and restart.",
     ["O rclone não foi encontrado. O FastSync precisa dele para falar com a nuvem. Instale-o (rclone.org) e reinicie."]),
    ("Main", "No accounts yet", ["Nenhuma conta ainda"]),
    ("Main", "Add an account and choose a folder: its files stay in sync both ways and available offline.",
     ["Adicione uma conta e escolha uma pasta: os arquivos ficam sincronizados nos dois sentidos e disponíveis offline."]),
    ("AccountCard", "Up to date", ["Atualizado"]),
    ("AccountCard", "Syncing…", ["Sincronizando…"]),
    ("AccountCard", "Paused", ["Pausado"]),
    ("AccountCard", "Needs attention", ["Precisa de atenção"]),
    ("AccountCard", "Stopped", ["Parado"]),
    ("AccountCard", "{} of {} files would be deleted. Press Sync now to confirm.",
     ["{} de {} arquivos seriam apagados. Clique em Sincronizar agora para confirmar."]),
    ("AccountCard", "Resume", ["Retomar"]),
    ("AccountCard", "Pause", ["Pausar"]),
    ("AccountCard", "Sync now", ["Sincronizar agora"]),
    ("AccountCard", "Open folder", ["Abrir pasta"]),
    ("AccountCard", "Open in browser", ["Abrir no navegador"]),
    ("AccountCard", "Remove account", ["Remover conta"]),
    # The Add account dialog.
    ("AddDialog", "Add account", ["Adicionar conta"]),
    ("AddDialog", "Service", ["Serviço"]),
    ("AddDialog", "Local folder", ["Pasta local"]),
    ("AddDialog", "Folder in the cloud (empty for all of it)", ["Pasta na nuvem (vazio para tudo)"]),
    ("AddDialog", "Everything", ["Tudo"]),
    ("AddDialog", "Email", ["E-mail"]),
    ("AddDialog", "Password", ["Senha"]),
    ("AddDialog", "Logging in…", ["Entrando…"]),
    ("AddDialog", "Finish the login in your browser…", ["Conclua o login no navegador…"]),
    ("AddDialog", "Use my own client ID (advanced)", ["Usar meu próprio ID de cliente (avançado)"]),
    ("AddDialog", "Avoids the shared limits of the built-in one, which can stop syncing for a while.",
     ["Evita os limites compartilhados do ID embutido, que podem interromper a sincronização por um tempo."]),
    ("AddDialog", "Client ID", ["ID do cliente"]),
    ("AddDialog", "Client secret", ["Segredo do cliente"]),
    ("AddDialog", "How to get one", ["Como obter um"]),
    ("AddDialog", "Cancel", ["Cancelar"]),
    ("AddDialog", "Log in", ["Entrar"]),
    # Preferences and About.
    ("SettingsDialog", "Preferences", ["Preferências"]),
    ("SettingsDialog", "Appearance", ["Aparência"]),
    ("SettingsDialog", "System", ["Sistema"]),
    ("SettingsDialog", "Light", ["Claro"]),
    ("SettingsDialog", "Dark", ["Escuro"]),
    ("SettingsDialog", "Language", ["Idioma"]),
    ("SettingsDialog", "About", ["Sobre"]),
    ("SettingsDialog", "Close", ["Fechar"]),
    ("About", "Two-way folder sync for Dropbox, Google Drive, OneDrive and MEGA. Light and fast.",
     ["Sincronização de pastas nos dois sentidos para Dropbox, Google Drive, OneDrive e MEGA. Leve e rápido."]),
    # The About card of the Fast family kit (about.slint): the same strings, in the same context, in every app.
    ("AboutCard", "Version {}", ["Versão {}"]),
    ("AboutCard", "Author: {}", ["Autor: {}"]),
    ("AboutCard", "License: {}", ["Licença: {}"]),
    ("AboutCard", "Close", ["Fechar"]),
]

q = lambda s: '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
for i, lang in enumerate(LANGS):
    d = f"{OUT}/{lang}/LC_MESSAGES"
    os.makedirs(d, exist_ok=True)
    with open(f"{d}/fastsync.po", "w") as f:
        f.write(f'msgid ""\nmsgstr ""\n"Content-Type: text/plain; charset=UTF-8\\n"\n"Language: {lang}\\n"\n')
        for ctx, msgid, tr in T:
            assert len(tr) == len(LANGS) and tr[i].count("{}") == msgid.count("{}"), msgid
            f.write(f"\nmsgctxt {q(ctx)}\nmsgid {q(msgid)}\nmsgstr {q(tr[i])}\n")
