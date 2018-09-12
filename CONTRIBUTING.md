# Contributing

First off, thanks for taking the time to contribute! This guide will answer
some common questions about how this project works.

While this is a Pinterest open source project, we welcome contributions from
everyone. Regular outside contributors can become project maintainers.

## Code of Conduct

Please read and understand our [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). We
work hard to ensure that our projects are welcoming and inclusive to as many
people as possible.

## Making Changes

1. Fork this repository to your own account
2. Make your changes and verify that tests pass
3. Commit your work and push to a new branch on your fork
4. Submit a pull request
5. Participate in the code review process by responding to feedback

Once there is agreement that the code is in good shape, one of the project's
maintainers will merge your contribution.

To increase the chances that your pull request will be accepted:

- Follow the coding style
- Write tests for your changes
- Write a good commit message

## Coding Style

This project follows [PEP 8](https://www.python.org/dev/peps/pep-0008/)
conventions and is linted using [flake8](http://flake8.pycqa.org/).

## Testing

The tests use [pytest](https://docs.pytest.org/) and can be run using `tox` or
directly via:

    py.test pymemcache/test/

Note that the tests require a local memcached instance.

## License

By contributing to this project, you agree that your contributions will be
licensed under its [Apache 2 license](LICENSE).
