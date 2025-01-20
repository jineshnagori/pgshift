from setuptools import setup, find_packages

setup(
    name="pgshift",
    version="0.1.0",
    description="A robust PostgreSQL migration tool",
    author="Upendra Dhanerwa",
    author_email="upendrajangir9@gmail.com",
    license="MIT",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "python-json-logger>=2.0,<3.3",
        "setuptools>=75.8.0"
    ],
    entry_points={
        "console_scripts": [
            "pgm = pgshift.cli:cli",
        ],
    },
)