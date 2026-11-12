# UI style for the Fast apps

The Fast apps share one icon style (`ICON-STYLE.md`) and one look inside the window. A person who knows one app should find their way around the next without thinking. This page lists what is shared. `crates/app/ui/family.slint` holds the values in code.

## What stays the same

All apps use Slint's built-in Fluent style with the software renderer, and take their colors from `Palette.*`, so they follow the desktop's light or dark mode and accent color. Do not draw a custom theme.

### Window

- The menu button (`MenuButton` in `controls.slint`: a borderless 28 px ☰) sits at the top right of the window and opens Preferences. The one exception is an app with a left sidebar (FastCheckup, FastHello, FastApiClient): the button sits at the right end of the sidebar header, which is the top of the window.
- Quitting and About live in Preferences, and in the footer of apps that have one: Quit and About on the left, the main actions on the right. The main action is `primary`.
- Esc closes the topmost dialog or overlay. A click outside a dialog closes it.
- Dialogs are overlays inside the window, not extra windows (see `Dialog` in `main.slint`).
- **One window.** An app runs once. Starting it again brings the open window forward and the new process leaves. This is `src/instance.rs`, the same file in every app: call `instance::send("show")` first thing in `main()` (skipped for fixture runs), and `instance::claim("show", ...)` once the window exists, with a handler that calls `instance::raise(ui.window())`. The popups (FastClipboard, FastMoji) use their own commands and focus code, but the same file.

### Preferences

- The name is **Preferences**, in the menu, the title and the translations. Not "Settings", "Options" or "Configuration".
- It is one dialog. Changes apply as soon as they are made, and the only button is **Close**. Do not add Save and Cancel, unless a value can be rejected after it is typed (a shortcut that is already taken is the one known case). Then keep the field's own Apply button instead of making the whole dialog wait. Every app follows this, the popups included: FastClipboard checks its shortcut when the key is pressed (a taken one shows an error and the old one stays) and limits "Entries kept" with a SpinBox.
- **Popups** (FastClipboard, FastMoji) hide when they lose focus, except while Preferences or About is open: then they stay until Close or Esc, so the person can copy something from another window.
- The first row is **Appearance**: System, Light or Dark. The second line of text says what System means. The language row follows it.
- App-specific options come after those two rows. About is a button in the footer of the dialog.
- The card is `Family.dialog-width` (380 px) wide, with 16 px padding and 12 px between rows. Every option is a `PrefRow` (`about.slint`): the label on the left, the control on the right at `Family.control-width` (190 px). A switch or a row of swatches keeps its own size. Never put a label above its control. A hint goes under its row in `Family.caption` and `Family.muted`.
- The footer has Quit (only apps that need it) and About on the left, and Close on the right.

### About

Every About dialog is the same `AboutCard` (`about.slint`) in the same card width, so a person sees the same thing in every app:

1. the app icon, 64 px
2. the name
3. `Version x.y.z`
4. one sentence about what the app does (the app's own, translated)
5. `Author: evandro777`
6. `License: GPL-3.0-or-later`
7. the address, like `github.com/evandro777/FastClipboard`, as a link in `Family.info` that opens the browser
8. **Close** on the right

There is no Website button: the link does that job. Author and license are fixed in the card. The app passes only its icon, name, version, description and address. About opens from the **About** button in Preferences, and Close returns to where the person was.

### Colors

Take every status color from `Family`, never a hex value in a page:

| Meaning | Property | Dark | Light |
|---|---|---|---|
| Error, dangerous action, high load | `Family.danger` | `#e5484d` | `#c9332b` |
| Warning, needs attention | `Family.warning` | `#e5a50a` | `#a86f00` |
| Done, healthy | `Family.success` | `#3fb950` | `#1a7f37` |
| Information, links | `Family.info` | `#5b97f7` | `#2f6fdb` |
| Secondary text | `Family.muted` | foreground at 70% | foreground at 70% |

Which column applies is decided by what the window actually draws (`Family.dark` reads the background), so it is right when the person picks Light or Dark in Preferences and when they go back to System. Do not test `Palette.color-scheme` in a page: it is `unknown` after "System".

The app's own color (its icon tile) is never used inside the window. The accent comes from the desktop. Color is never the only sign of status: add a word or an icon. Colors that tell data series apart in a chart are not status colors, and may stay as literal values.

### Type

Four sizes and two weights. Nothing else.

| Use | Size | Property |
|---|---|---|
| Hints, footers, table captions | 11 px | `Family.caption` |
| Body text, lists, tables | 13 px | `Family.body` |
| Group titles | 16 px | `Family.title` |
| Dialog and page titles | 20 px | `Family.page-title` |

Names and section titles are weight 600 (`Family.section-weight`). Page and dialog titles are 700 (`Family.page-weight`). Do not use 500, or anything heavier than 700.

A big number or greeting that is the content of the page (the "Hello, Ana" demo) may be larger. It is content, not interface text.

### Tabs, selection and tables

- **Page tabs are underlined** (`TabItem` in `controls.slint`: bold title, 2 px accent underline, a 1 px `Palette.border` line under the row). **Filters and modes are a segmented control** (FastCheckup's `FilterBar`). A filled `primary` button is never a tab: `primary` is for the one main action of a view.
- **The picked row is quiet**: `Family.selected` (the accent at 26 % in dark, 18 % in light) plus a `Family.selected-bar` wide accent bar on the left edge. The text keeps `Palette.foreground`. Never fill a row or a menu entry with `Palette.selection-background`.
- Tables: a 1 px `Palette.border` under the header, rows of one fixed height, a hairline or a faint alternate tint between rows, numbers right-aligned. Editable cells stay `LineEdit`: it follows the caret in a long value and has the Cut/Copy/Paste menu, which a bare `TextInput` does not.
- **Every tab row is a `TabBar` of `TabItem`s** (32 px, left-aligned, hairline under it), never Fluent's `TabWidget`, which stretches the tabs over the width. Pages under it stay alive and are only hidden with `visible`, so each keeps its scroll position.
- A badge is neutral (`NeutralBadge`: `Family.muted` text, 1 px border, no fill) unless it states a status (then a tinted chip). A source or a category is not a status.
- **The accent marks "active".** The bar and underline of the open tab or nav item use `Palette.accent-background`; the label stays `Palette.foreground`. `Family.info` is for links only.
- Filters and modes with up to four options use `FilterBar` (also a short interval list like 1/2/5 s). A longer list stays a `ComboBox`.
- Search fields: `SearchBox` (`controls.slint`), the caller passes the placeholder. Esc clears it only when it holds text, so a popup can still close on Esc. A page that needs keys before the text field (Up/Down/Delete/Menu of a list) sets the `key` callback: it runs first, and `accept` stops the key there.
- Dialog card: `Family.scrim` behind it, then a card with `Palette.background`, `Family.radius`, a 1 px `Palette.border`, padding 16 and a width of `min(Family.dialog-width, parent.width - 32px)`. Never `drop-shadow-*`: the software renderer ignores it, so the card would have no edge.
- **Controls in a row are `Family.control-height` (32 px)**: Fluent's Button, ComboBox, LineEdit and SpinBox already are, and `SearchBox` and `FilterBar` match them. A `VerticalLayout` inside a `ScrollView` has `alignment: start`, or a card taller than its rows stretches every row (FastClipboard's Preferences fields once grew to 36 px).

### Space and shape

Gaps are 4, 8, 12 or 20 px (`Family.gap-*`). Panels and dialogs use the corner radius `Family.radius`; small controls (nav items, segments, buttons, search boxes) use `Family.control-radius`. Rows of one kind have the same height and gaps.

### Icons in the window

Line icons with a 2 px stroke and round caps, drawn on a 24 px grid and tinted with `colorize-icon: true`, so they follow the theme. Use the app's own `ui/icons/*.svg`; the shared set (search, refresh, trash, copy, power, folder, info, circle-check, chevrons) comes with the kit; copy into an app only the icons it uses (an unused file is dead weight), and FastCheckup has the largest set to copy from. The `Icon` component is in `controls.slint`. Do not use text symbols (▾, ✕, ×, ⚠, "+ Add") as icons or in labels. `controls.slint` draws the small ones (`CloseButton`, `StepButton`, `PlusIcon`, `AlertIcon`, `MenuButton`) with paths, so an app needs no icon files for them.

### Words

- Sentence case everywhere: "Clear history", not "Clear History".
- A button says what it does: "Save", "Clear history…". It never says "OK".
- The same action has the same name everywhere: if the button says "Clear", the message afterwards says "Cleared".
- Errors say what went wrong and what to do. They do not apologize.
- Every string goes through `@tr()` or the `Tr` global (see `AGENTS.md`).

## Using it in a new app

1. `prepare.sh` keeps `family.slint`, `about.slint` and `controls.slint`. Leave them as they are.
2. `import { Family } from "family.slint";` in the files that need it. Use `AboutCard` and `PrefRow` from `about.slint` for the About dialog and the Preferences rows, and `MenuButton`, `TabBar`, `TabItem`, `CloseButton`, `Icon`, `NeutralBadge`, `FilterBar`, `SearchBox` from `controls.slint` (FastHello's Widgets page, Inputs tab, shows the last four).
3. Check the rules above with `grep -nE '#[0-9a-fA-F]{6}|font-size: *[0-9]+px|font-weight: *[0-9]+' crates/app/ui/*.slint`. Every hit should be inside `family.slint` or be content, not interface text.
4. Take screenshots of the main page and of Preferences with `scripts/ui-snapshot.sh`, in light and dark, and compare them with FastHello's.

## Changing the kit

Change FastHello's `family.slint`, `about.slint` or `controls.slint`, raise the version in the first line of `family.slint`, and copy the files to every other Fast app in the same change. `instance.rs` follows the same rule: change FastHello's and copy it to all six (`cmp` them to check). Strings in `about.slint` are translated in the context `AboutCard`: every app's `dev/gen_po.py` needs the same four entries (`Version {}`, `Author: {}`, `License: {}`, `Close`). If an app needs a value the kit does not have, add it to the kit first.

## Where the apps stand

This list is for whoever works on the apps next. Remove a line when it is done.

- **Open across the family (kit v8):** FastClipboard's footer hint still joins its keys with " · ". Plain `LineEdit` stays for form fields (URL, key/value cells, dialog inputs); only search and filter boxes are `SearchBox`.

- **FastHello:** follows this page.
- **FastClipboard:** follows this page.
- **FastProcMan:** follows this page. The colors of the chart series are literal on purpose, because they are data, not status.
- **FastMoji:** follows this page. The skin tone swatches are literal on purpose, because they are data, not status.
- **FastApiClient:** follows this page. The request method colors in the sidebar are literal on purpose: they label data, and PATCH has no matching status color.- **FastCleany:** follows this page. Its `Theme` global keeps only what is specific to it: the grey of an unscanned area, a purple for categories and the area-level helper. The headline values of the Overview cards stay at 18 px because they are content. It has no screenshot in the repository yet.
