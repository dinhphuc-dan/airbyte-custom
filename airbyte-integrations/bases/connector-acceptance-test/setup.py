#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import setuptools

MAIN_REQUIREMENTS = [
    "airbyte-cdk",
    "docker",
    "PyYAML",
    "icdiff",
    "inflection",
    "pdbpp",
    "pydantic",
    "pytest",
    "pytest-sugar",
    "pytest-timeout",
    "pprintpp",
    "dpath",
    "jsonschema",
    "jsonref",
    "deepdiff",
    "requests-mock",
    "pytest-mock",
    "pytest-cov",
    "hypothesis",
    "hypothesis-jsonschema",  # TODO alafanechere upgrade to latest when jsonschema lib is upgraded to >= 4.0.0 in airbyte-cdk and connector acceptance tests
    # Pinning requests and urllib3 to avoid an issue with dockerpy and requests 2.
    # Related issue: https://github.com/docker/docker-py/issues/3113
    "urllib3",
    "requests",
]

setuptools.setup(
    name="connector-acceptance-test",
    description="Contains acceptance tests for connectors.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    url="https://github.com/airbytehq/airbyte",
    packages=setuptools.find_packages(),
    install_requires=MAIN_REQUIREMENTS,
)
