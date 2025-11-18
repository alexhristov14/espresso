from setuptools import setup, find_packages

setup(
    name="espresso-scheduler",
    version="0.1.0",
    description="A flexible job scheduler library",
    author="Alexander Hristov",
    packages=find_packages(exclude=["testing", "jobs_definitions"]),
    install_requires=[
        "pyyaml",
        "aio-pika",
        "fastapi",
        "uvicorn[standard]",
        "pydantic",
    ],
    extras_require={
        "dev": [
            "ruff",
            "pytest",
            "pytest-asyncio",
        ],
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
