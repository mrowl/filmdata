try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='filmdata',
    version='0.1',
    description='',
    author='Scott Snyder',
    author_email='',
    url='',
    install_requires=[
        "SQLAlchemy>=0.6",
        "oauth2",
    ],  
    dependency_links= ['https://github.com/simplegeo/python-oauth2/tarball/master#egg=oath2'],
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    test_suite='nose.collector',
    package_data={'filmdata': ['i18n/*/LC_MESSAGES/*.mo']},
    #message_extractors={'filmlust': [
    #        ('**.py', 'python', None),
    #        ('public/**', 'ignore', None)]},
    zip_safe=False,
)
