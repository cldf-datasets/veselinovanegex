from setuptools import setup


setup(
    name='cldfbench_veselinovanegex',
    py_modules=['cldfbench_veselinovanegex'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'veselinovanegex=cldfbench_veselinovanegex:Dataset',
        ]
    },
    install_requires=[
        'cldfbench[glottolog]',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
