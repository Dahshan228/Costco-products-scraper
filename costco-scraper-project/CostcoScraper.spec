# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['costco_gui.py'],
    pathex=[],
    binaries=[],
    datas=[('costco_scraper.py', '.'), ('urls_part1.json', '.'), ('urls_part2.json', '.')],
    hiddenimports=['numbers', 'cmath'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='CostcoScraper',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
app = BUNDLE(
    exe,
    name='CostcoScraper.app',
    icon=None,
    bundle_identifier=None,
)
