import setuptools
import os
from pathlib import Path

with open("README.md", "r") as fh:
    long_description = fh.read()

file_path = os.path.join(Path('.'),"requirements.txt")
import pkg_resources
with open(file_path) as requirements_txt:
    install_requires = [
        str(requirement)
        for requirement
        in pkg_resources.parse_requirements(requirements_txt)
    ]

lib = "traiding.cache.mongodb"
package_arr = [f"{lib}.{pkg}" for pkg in setuptools.find_packages(where="src")]
package_arr.append(lib)

setuptools.setup(
    name=lib,
    version="1.0.3",
    author="InsonusK",
    author_email="insonus.k@gmail.com",
    description="Framework for saving cache DataFrame to MongoDB",
    long_description=long_description,
    url="https://github.com/NNTrade/MongoDB-cache",
    packages=package_arr,
    package_dir={lib:'src'},
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)