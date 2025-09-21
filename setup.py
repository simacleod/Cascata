from setuptools import setup, find_packages, Extension

extensions = [
    Extension(
        "cascata._async_queue",
        sources=["cascata/_async_queue.cpp"],
        extra_compile_args=["-std=c++17", "-O3", "-DNDEBUG"],
    )
]

setup(
    name='cascata',
    version='0.1.8',
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
    python_requires='>=3.10',
    install_requires=[
        'multiprocess',
        'dill',
        'aioprocessing',
        'aioitertools',
        'networkx',
    ],
    include_package_data=True,
    zip_safe=False,
    ext_modules=extensions,
)
