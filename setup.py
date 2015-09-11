from setuptools import setup, find_packages


setup(
    name='datamapper',
    version='0.1',
    description="Map database queries to JSON schema objects.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='schema jsonschema json data sql',
    author='OCCRP',
    author_email='tech@occrp.org',
    url='http://github.com/occrp/datamapper',
    license='AGPLv3',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'jsonmapping',
        'sqlalchemy',
        'pyyaml',
        'click'
    ],
    tests_require=[
        'nose',
        'coverage',
        'unicodecsv',
        'dataset',
        'normality'
    ],
    entry_points={
        'console_scripts': [
        ]
    }
)
