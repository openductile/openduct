# Security policy

OpenDuct carries data for systems that cannot afford to lose it or have it read by the wrong party. We take every
report seriously.

## Reporting a vulnerability

**Do not open a public issue, discussion or pull request for a security problem.**

Report it privately through GitHub: on this repository, open the **Security** tab and choose **Report a
vulnerability**. Only the maintainers see it. Please include what you found, the version or commit, how to
reproduce it, and what an attacker could do with it.

We will:

- acknowledge your report within **3 working days**;
- tell you within **10 working days** whether we confirm it and how we plan to fix it;
- keep you informed until it is fixed, and credit you in the advisory unless you ask us not to.

Please give us a reasonable time to release a fix before you disclose the problem publicly.

## Supported versions

OpenDuct is in early development and has no stable release yet. Security fixes go to the `main` branch. Once
releases begin, this section will list the versions that receive fixes.

## Scope

OpenDuct listens on the network and stores data. Reports we especially want to hear about include anything that:

- crashes or hangs the broker from the network, or exhausts its memory, disk or connections without limit;
- reads, changes or loses data without authorization, or acknowledges a write it has not stored;
- escapes the data directory or otherwise touches files it should not.

Today OpenDuct speaks plaintext only, with no authentication: run it on a trusted network. TLS and authentication
are on the roadmap (`README.md`).
