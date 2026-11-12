use std::{env, fs, path::Path};

/// The tray PNGs (`tray/*.svg` rendered at 64 px, see AGENTS.md) as raw ARGB32, which is what StatusNotifier wants,
/// so the app needs no PNG decoder at run time.
fn tray_pixmaps() {
    let out = env::var("OUT_DIR").unwrap();
    for name in ["idle", "syncing", "paused", "error"] {
        let src = format!("tray/{name}.png");
        let mut reader = png::Decoder::new(std::io::BufReader::new(fs::File::open(&src).unwrap()))
            .read_info()
            .unwrap();
        let mut buf = vec![0; reader.output_buffer_size().unwrap()];
        let info = reader.next_frame(&mut buf).unwrap();
        assert!(
            info.color_type == png::ColorType::Rgba && info.bit_depth == png::BitDepth::Eight,
            "{src} must be 8-bit RGBA"
        );
        let argb: Vec<u8> = buf[..info.buffer_size()]
            .as_chunks::<4>()
            .0
            .iter()
            .flat_map(|p| [p[3], p[0], p[1], p[2]])
            .collect();
        fs::write(Path::new(&out).join(format!("{name}.argb")), argb).unwrap();
    }
}

fn main() {
    println!("cargo:rerun-if-changed=ui");
    println!("cargo:rerun-if-changed=translations");
    println!("cargo:rerun-if-changed=tray");
    tray_pixmaps();
    let config = slint_build::CompilerConfiguration::new()
        .with_style("fluent".into())
        .with_bundled_translations("translations");
    slint_build::compile_with_config("ui/main.slint", config).unwrap();
}
