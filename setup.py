from setuptools import setup, find_packages

setup(name="zeroman",
    version="0.1",
    author="Justin Azoff",
    author_email="justin@bouncybouncy.net",
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    zip_safe=False,
    install_requires=[
        "zmq",
    ],
    entry_points = {
        "zeromanager    =   zeroman.manager:main",
    },
    test_suite='nose.collector',
)
