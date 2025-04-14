from setuptools import setup, find_packages

setup(
    name='message_queue_client',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pika',  # Required for RabbitMQ implementation
        # Add other dependencies as needed
    ],
    author='Your Name',
    author_email='your.email@example.com',
    description='A reusable message queue client abstraction for microservices.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/message_queue_client',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)
