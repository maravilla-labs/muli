# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Muli, please report it responsibly. **Do not open a public issue.**

**Email:** security@maravillalabs.com

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fix (optional)

## Disclosure Timeline

- **Acknowledgment:** within 48 hours of report
- **Initial assessment:** within 5 business days
- **Fix for critical issues:** within 7 days
- **Fix for non-critical issues:** within 30 days
- **Public disclosure:** coordinated with the reporter after a fix is released

We follow [responsible disclosure](https://en.wikipedia.org/wiki/Responsible_disclosure) principles. We ask reporters to allow us reasonable time to address issues before public disclosure.

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

Only the latest patch release of each supported minor version receives security fixes.

## Scope

The following are in scope:
- The `muli-server` and `muli-agent` binaries
- The embedded registry (OCI, npm, Cargo) and git hosting service
- Authentication and authorization mechanisms
- Container isolation and sandboxing

Out of scope:
- Vulnerabilities in upstream dependencies (report these to the upstream project, but do let us know)
- Issues requiring physical access to the host
- Social engineering attacks

## References

For guidance on security policies in open-source projects, see:
- [GitHub Security Advisories](https://docs.github.com/en/code-security/security-advisories)
- [OpenSSF Vulnerability Disclosure Guide](https://github.com/ossf/oss-vulnerability-guide)
- [Muli Security Model](docs/security-model.md)
