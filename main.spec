# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[('TradeSettings.csv', '.'), ('Credentials.csv', '.'), ('xtspythonclientapisdk', 'xtspythonclientapisdk'), ('xtspythonclientapisdk/config.ini', 'xtspythonclientapisdk'), ('.venv\\Lib\\site-packages\\fyers_apiv3\\FyersWebsocket\\map.json', 'fyers_apiv3\\FyersWebsocket')],
    hiddenimports=['xtspythonclientapisdk', 'xtspythonclientapisdk.Connect', 'configparser', 'requests', 'urllib3', 'fyers_apiv3'],
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
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
