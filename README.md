# 仮想環境

## create 仮想環境

1. `uv init`
2. `uv venv`
3. `.venv/Scripts/activate`
4. `uv add numpy pandas matplotlib xarray cartopy netCDF4 h5netcdf ipykernel`  
   これで uv の中でこの仮想環境の情報が保存される

## 仮想環境の再現

`uv sync`

## 仮想環境の実行/停止

`.venv/Scripts/activate`  
`deactivate`

# directory

- C-BandRadar
- Persivel
