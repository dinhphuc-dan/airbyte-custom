#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.82.0",
    "google-cloud-storage==3.1.0",
    "fastavro==1.10.0",
    "pyarrow==16.1.0",
    "unstructured==0.10.27",
    "smart_open==7.1.0",
]

TEST_REQUIREMENTS = []

setup(
    name="source_gcs_custom",
    description="Source implementation for Gcs Custom.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
