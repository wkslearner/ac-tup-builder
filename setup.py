from setuptools import setup, find_packages

setup(name='ac-tup-builder',
      version='1.0.0',
      description='Tag-based User Profile Builder project.',
      url='http://www.andpay.me',
      author='Sea.Bao',
      author_email='sea.bao@andpay.me',
      license='Private',
      packages=find_packages(),
      install_requires=[
            'ti-srv-cfg-python',
            'ti-daf-python',
            'ti-lnk-python',
            'bi-common-util',
            'dpath',
            'lru-dict',
            'pandas'],
      zip_safe=False,
      test_suite='nose.collector',
      tests_require=['nose'],
      scripts=[],
      entry_points = {
        'console_scripts': [],
      },
      include_package_data=True
      )
