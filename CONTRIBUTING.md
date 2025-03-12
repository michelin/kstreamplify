# Contributing to Kstreamplify

Welcome to our contribution guide.
This guide will help you understand the process and expectations for contributing.

## Getting Started

### Issues

If you want to report a bug, request a feature, or suggest an improvement, please open an issue on the [GitHub repository](https://github.com/michelin/kstreamplify/issues)
and fill out the appropriate template.

If you find an existing issue that matches your problem, please:
- Add your reproduction details to the existing issue instead of creating a duplicate.
- Use reactions (e.g., 👍) on the issue to signal that it affects more users. [GitHub reactions](https://github.blog/news-insights/product-news/add-reactions-to-pull-requests-issues-and-comments/) help maintainers prioritize issues based on user impact.

If no open issue addresses your problem, please open a new one and include:
- A clear title and detailed description of the issue.
- Relevant environment details (e.g., version, OS, configurations).
- A code sample or executable test case demonstrating the expected behavior that is not occurring.

### Pull Requests

To contribute to Kstreamplify:

- Fork the repository to your own GitHub account
- Clone the project to your machine
- Create a branch from the `main` branch
- Make your changes and commit them to your branch
- FPush your changes to your fork
- Open a merge request to the `main` branch of the Kstreamplify repository so that we can review your changes

## Style Guide

We maintain a consistent code style using [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven).
For Java code, we follow the [Palantir](https://github.com/palantir/palantir-java-format) style.

To check for formatting issues, run:

```bash
mvn spotless:check
```

To automatically fix formatting issues and add missing file headers, run:

```bash
mvn spotless:apply
```