# Security Policy

## Supported Versions

Use this section to tell people about which versions of your project are currently being supported with security updates.

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability within ETL Lineage Analyzer, please send an email to [your-email@example.com]. All security vulnerabilities will be promptly addressed.

Please include the following information in your report:

- Type of issue (buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the vulnerability
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

This information will help us quickly assess and address the vulnerability.

## Security Best Practices

When using ETL Lineage Analyzer:

1. **Keep dependencies updated**: Regularly update the sqlparse library and other dependencies
2. **Validate input files**: Ensure that the ETL scripts you're analyzing are from trusted sources
3. **Review output carefully**: Always review the generated lineage reports for accuracy
4. **Use in isolated environments**: When analyzing sensitive ETL scripts, consider using isolated environments

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine the affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release new versions with the fixes
5. Publicly announce the vulnerability and the fix

## Credits

We would like to thank all security researchers and users who responsibly disclose vulnerabilities to us. 