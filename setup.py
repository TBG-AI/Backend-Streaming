from setuptools import setup, find_packages

# Read requirements.txt
with open('requirements.txt') as f:
    requirements = [
        line.strip()
        for line in f
        if line.strip() and not line.startswith('#')
    ]

setup(
    name="backend_streaming",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    python_requires=">=3.6",
)