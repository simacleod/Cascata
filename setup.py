from setuptools import setup, find_packages

setup(
    name='cascata',
    version='0.1.3',
    author='Stuart MacLeod',
    author_email='orangootan@gmail.com',
    description='An asynchronous, multicore graph execution framework for Python.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/simacleod/Cascata',
    project_urls={
        'Bug Tracker': 'https://github.com/simacleod/Cascata/issues',
    },
    license='MIT',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'Framework :: AsyncIO',
    ],
    python_requires='>=3.7',
    install_requires=[
        'multiprocess',
        'dill',
        'aioprocessing',
        'aioitertools',
        'networkx',
    ],
    include_package_data=True,
    zip_safe=False
)
