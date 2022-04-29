# -*- mode: python ; coding: utf-8 -*-
import sys
sys.setrecursionlimit(5000)


import os.path
import pysmi
import pysnmp.smi.mibs

def module_base(module):
	return os.path.abspath(os.path.join(module.__file__, os.pardir))

from PyInstaller.utils.hooks import collect_data_files, collect_submodules
block_cipher = None


a = Analysis(['C:/Users/Wkhan/Desktop/NMSv4/Final NMS/Switch_Analysisv1.0.0.py'],
             binaries=[],
             datas=[('C:/Users/Wkhan/Desktop/NMSv4/Final NMS/SwitchUtilization_08_02_2021.py', '.'), ('C:/Users/Wkhan/Desktop/NMSv4/Final NMS/NMSSQL.xml', '.')],
             hiddenimports=['pysnmp.smi.exval','pysnmp.cache'] + collect_submodules('pysnmp.smi.mibs') + collect_submodules('pysnmp.smi.mibs.instances'),
			 hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

xx = Tree('C:/ProgramData/Anaconda3/lib/site-packages/pysnmp/smi/mibs',prefix='pysnmp/smi/mibs',excludes='.py')
pyz = PYZ(a.pure,a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
		  a.scripts,
          [],
          exclude_binaries=True,
          name='Switch_Analysis1.0.0',
          debug=False,
          strip=None,
          upx=True,
          console=False )
		  
x = Tree('C:/ProgramData/Anaconda3/lib/site-packages/pysnmp/smi/mibs',prefix='pysnmp/smi/mibs')
y = Tree('C:/ProgramData/Anaconda3/lib/site-packages/pysmi',prefix='pysmi')
z = Tree('C:/ProgramData/Anaconda3/Lib/site-packages/pysnmp_mibs')
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               x,
			   y,
			   xx,
			   z,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='Switch_Analysisv1.0.0')
