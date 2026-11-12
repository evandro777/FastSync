# Icon style for the Fast apps

Every Fast app uses the same icon shape, so the set is recognisable in a dock, a tray or an app grid. This page has the rules and the reasons for them. Follow it when you draw the icon of a new app.

## Why one style

The Fast apps share a goal: light, fast and simple. Their icons did not. Three had their own look (a blue clipboard with a bolt, a dark graph tile, a yellow smiley with a bolt) and three were the same blue placeholder square. Next to each other they looked like six unrelated projects.

The shared style has three parts that stay fixed, and two that change per app:

| Fixed in every app                              | Changes per app |
|-------------------------------------------------|-----------------|
| Tile shape, with a stepped top-right corner     | Tile color      |
| White symbol, same size, same stroke, same lean | The symbol      |

The stepped corner is the family mark. Two steps, a long one and then a short one, echo the arms of the F that starts every name. It is a shape only, with no letter, so it stays clear at small sizes.

Some choices were made on purpose:

- **No lightning bolt.** Two apps used one, and nearly every "fast" logo has one. Speed is shown by the forward lean and the cut corner.
- **No letters in the icon.** An F at 16 px is unreadable. The symbol has to tell the apps apart at that size, and the corner step ties them together.
- **One flat color per tile.** Solid colors are easy to tell apart in a dock, and a flat color survives conversion to PNG (see "What not to draw").

## The rules

All numbers are in a 128 by 128 grid.

### Tile

- Rounded square, corner radius 26 on the left side and the bottom right.
- The top-right corner is stepped: the top edge ends at x 64, drops 14, runs to x 92, drops 14, then runs to x 128.
- Use this path as is:

```txt
M26 0H64V14H92V28H128V102A26 26 0 0 1 102 128H26A26 26 0 0 1 0 102V26A26 26 0 0 1 26 0Z
```

### Symbol

- White (`#ffffff`), one symbol per app, drawn inside a 64 by 64 box centered at (62, 68). The center is a little left of and below the tile's center because the cut corner takes weight from the top right.
- Strokes are 10 units wide with round caps and round joins. Small details (a wave, a thin line) may use 8.
- Filled shapes, such as dots, may be used next to strokes. Keep them at least 12 units across.
- The symbol leans forward: skew it 8° around its center, with `skewX(-8)`. The tile stays upright.
- One idea per icon. If it needs a second object to be understood, simplify it.
- Check it at 16, 32 and 128 px. It must still be recognisable at 16.

### Color

- One flat color per app. White on the tile must reach a contrast of at least 3:1.
- Pick a hue that no other Fast app uses. Keep it about as strong as the others: not pastel, not near black.

| App | Tile | Symbol | White on tile |
| --- | --- | --- | --- |
| FastClipboard | `#2f6fdb` | Clipboard | 4.8:1 |
| FastProcMan | `#1f9d63` | Pulse line | 3.5:1 |
| FastMoji | `#cc7b00` | Smile | 3.4:1 |
| FastApiClient | `#7a4fd6` | Braces with a dot | 5.4:1 |
| FastCleany | `#11998e` | Sparkle | 3.5:1 |
| FastSync | `#0b86a8` | Cloud with a two-way arrow | 4.2:1 |
| FastHello (template) | `#4b5568` | Speech bubble with a wave | 7.5:1 |

Hues that are still free, with their approximate contrast:

| Tile | Looks like | White on tile |
| --- | --- | --- |
| `#d4572a` | Red-orange | 4.0:1 |
| `#c2417f` | Pink | 4.8:1 |

When you use one, add your app to the first table in this file, in every Fast app that carries a copy.

### What not to draw

The PNG is made with ImageMagick, which only handles plain shapes. Do not use:

- gradients, opacity, filters or shadows
- `<use>`, `<symbol>`, `<mask>` or `<clipPath>` (draw the shapes out)
- text or letters
- an outline around the tile
- a lightning bolt

Gradients and opacity are not just ignored, they come out wrong, so the PNG would not match the SVG.

## Make the icon for a new app

1. After `./prepare.sh`, open `packaging/io.github.<owner>.<App>.svg`. It is the template icon: the slate tile with a speech bubble.
2. Change the tile `fill` to your color, and replace the contents of the `<g>` group with your symbol. Keep the group's `transform`, `stroke-width` and caps as they are.
3. Make the PNG (it is the AppImage icon, 256 px). From the repository root:

   ```sh
   convert -background none -density 384 packaging/io.github.<owner>.<App>.svg -resize 256x256 packaging/io.github.<owner>.<App>.png
   ```

   Look at the PNG next to the SVG. If they differ, something in "What not to draw" slipped in.
4. Check it small (16, 32 and 128 px) and on a light and a dark background.
5. Add the app and its color to the tables above, and to the copy of this file in the other Fast apps if you keep them in step.

## Template SVG

```svg
<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">
  <path d="M26 0H64V14H92V28H128V102A26 26 0 0 1 102 128H26A26 26 0 0 1 0 102V26A26 26 0 0 1 26 0Z" fill="#4b5568"/>
  <g transform="translate(62 68) skewX(-8) translate(-62 -68)" fill="none" stroke="#ffffff" stroke-width="10" stroke-linecap="round" stroke-linejoin="round">
    <!-- your symbol here, inside x 30..94 and y 36..100 -->
  </g>
</svg>
```

## Where the icon is used

- `packaging/<app id>.svg`: the Flatpak and AppImage icon, and the About dialog in the app.
- `packaging/<app id>.png`: the AppImage `.DirIcon`, and any place that needs a bitmap, such as a tray icon.
- FastSync only: `crates/app/tray/{idle,syncing,paused,error}.svg` are the tray icons, the same tile and cloud with another color and mark per status. `build.rs` turns their PNGs into pixmaps; regenerate a PNG with `convert -background none -density 768 <name>.svg -resize 64x64 PNG32:<name>.png` (`PNG32:` keeps it 8-bit RGBA, which `build.rs` checks).
- The landing page and the README may show it too. Update them when the icon changes.
