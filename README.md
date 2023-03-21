# tap-bitbucket

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [BITBUCKET CLOUD API](https://developer.atlassian.com/cloud/bitbucket/rest/intro/)
- Extracts the following resources:
  - [Repositories](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-repositories/#api-repositories-workspace-get)
  - [Pull Requests](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-get)
  - [Commits](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-commits-get)
- Outputs the schema for each resource

TO DO:
- Incrementally pulls data based on the input state

---

# Quick Start

1. Install
```commandline
pip install git+https://github.com/writ3it/tap-bitbucket.git
```
2. Create the config file. Example
```json
{
  "username": "writ3it",
  "password": "ChangeMe",
  "workspace": "Workspace"
}
```
3. Run the application `tap-bitbucket` can be run with:
```commandline
tap-bitbucket -c config.json
```
