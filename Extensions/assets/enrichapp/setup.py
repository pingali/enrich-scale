import os
from setuptools import find_packages
from setuptools import setup

requires = [
]

tests_require=[
   "pytest"
]
    
setup(name='enrichapp-discover',
      version='0.1',
      description='enrichapp package',
      url='https://github.com/pingali/enrich-scribble', 
      author='Venkata Pingali',
      author_email='pingali@scribbledata.io', 
      license='None',
      install_requires=requires,
      tests_require=tests_require,
      packages=find_packages('src'),
      package_dir={'': 'src'},      
      zip_safe=True,
      include_package_data=True,
      entry_points={
          'Manage.asset': ['enrichapp=enrichapp'],
      },
)
