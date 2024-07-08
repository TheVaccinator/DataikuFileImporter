from setuptools import setup, find_packages
import os

# Leer el contenido del README.md para la descripción larga
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Configuración detallada del paquete
setup(
    name="file_importer",
    version="0.1.0",
    author="Omar Khalil Abuali",
    author_email="tuemail@example.com",
    description="A utility for importing files from Sharepoint folders, supporting CSV, XLS, and XLSX formats.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tuusuario/file_importer",  # Reemplaza con la URL de tu repositorio
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.1.5",
        "dataiku>=8.0.0",
        "openpyxl>=3.0.0",
        "xlrd>=2.0.1",
    ],
    extras_require={
        "dev": [
            "unittest",
        ],
    },
    include_package_data=True,
    package_data={
        # Incluir archivos adicionales dentro de los paquetes, por ejemplo archivos de configuración o datos
        "": ["*.csv", "*.xlsx", "*.xls"],
    },
    entry_points={
        "console_scripts": [
            "file-importer=file_importer:main",  # Asumiendo que tienes un script ejecutable en file_importer.py
        ],
    },
)
