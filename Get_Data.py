import os
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil

# Lokasi file kaggle.json yang sudah kamu unduh
kaggle_json_path = './kaggle.json'

# Pastikan direktori ~/.kaggle sudah ada
kaggle_dir = os.path.join(os.path.expanduser('~'), '.kaggle')
os.makedirs(kaggle_dir, exist_ok=True)

# Salin kaggle.json ke ~/.kaggle/
shutil.copy(kaggle_json_path, os.path.join(kaggle_dir, 'kaggle.json'))

# Ubah permission agar tidak terbaca publik (khusus Linux/Mac)
os.chmod(os.path.join(kaggle_dir, 'kaggle.json'), 0o600)

# Inisialisasi dan autentikasi API
api = KaggleApi()
api.authenticate()

# Download dataset
dataset_name = 'jessemostipak/hotel-booking-demand'
download_path = './dags/data'
api.dataset_download_files(dataset_name, path=download_path, unzip=True)

print(f'The dataset was successfully downloaded and extracted in: {download_path}')