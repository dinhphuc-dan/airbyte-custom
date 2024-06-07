#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.82.0",
    "cryptography==41.0.7",
    "PyJWT==2.8.0"
]

TEST_REQUIREMENTS = []

setup(
    name="source_google_analytics_ga4_custom",
    description="Source implementation for Google Analytics Ga4 Custom.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
