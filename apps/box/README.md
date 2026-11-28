# Box

Box Platform provides functionality to provide access to content stored within Box.

Made by `openduct app from-openapi` from https://github.com/box/box-openapi/blob/c9699f404395/openapi.json (Apache-2.0 licence) (OpenAPI 3.0) on 2026-11-27. The vendor's API reference: <https://developer.box.com>.

## Signing in

- **Sign in with Box** (`box`): a sign-in at Box. Once for the installation, its admin registers an app (an OAuth client) in Box's pages for developers (https://developer.box.com), gives it the callback address shown under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`), allows the permissions the actions need, and enters its client ID and secret there. Then each workspace **Connects** its account under Credentials.

## Actions

- **List trashed items** (`list_items`): Retrieves the files and folders that have been moved to the trash.
- **Find metadata template by instance ID** (`list_metadata_templates`): Finds a metadata template by searching for the ID of an instance of the template.
- **List enterprise users** (`list_users`): Returns a list of all users for the Enterprise along with their user_id, public_name, and login.
- **List file app item associations** (`list_app_item_associations`): This is a beta feature, which means that its availability might be limited.
- **Create upload session** (`create_upload_session`): Creates an upload session for a new file.
- **Create folder** (`create_folder`): Creates a new empty folder within the specified parent folder.
- **Add initial classifications** (`create_schema_classification`): When an enterprise does not yet have any classifications, this API call initializes the classification template with an initial set of classifications.
- **Create user** (`create_user`): Creates a new managed user in an enterprise.
- **Create metadata taxonomy** (`create_metadata_taxonomy`): Creates a new metadata taxonomy that can be used in metadata templates.
- **List metadata taxonomy nodes** (`list_nodes`): Used to retrieve metadata taxonomy nodes based on the parameters specified.
- **Get file information** (`get_file`): Retrieves the details about a file.
- **Get folder information** (`get_folder`): Retrieves details for a folder, including the first 100 entries in the folder.
- **Get metadata template by ID** (`get_metadata_template`): Retrieves a metadata template by its ID.
- **Get user** (`get_user`): Retrieves information about a user in the enterprise.
- **Get metadata taxonomies for namespace** (`get_metadata_taxonomy`): Used to retrieve all metadata taxonomies in a namespace.
- **Add classification** (`update_schema_add`): Adds one or more new classifications to the list of classifications available to the enterprise.
- **Get current user** (`get_me`): Retrieves information about the user who is currently authenticated.
- **Restore file** (`update_file`): Restores a file that has been moved to the trash.
- **Restore folder** (`update_folder`): Restores a folder that has been moved to the trash.
- **Update user** (`update_user`): Updates a managed or app user in an enterprise.
- **Download file** (`get_content`): Returns the contents of a file in binary format.
- **Get trashed folder** (`get_folder_trash`): Retrieves a folder that has been moved to the trash.
- **Update metadata taxonomy** (`update_metadata_taxonomy`): Updates an existing metadata taxonomy.
- **Create metadata template** (`schema_metadata_template`): Creates a new metadata template that can be applied to files and folders.
- **Create metadata taxonomy levels** (`levels_metadata_taxonomy`): Creates new metadata taxonomy levels.

## Not done

- 4 operations left out: a file upload (multipart)
- 266 more operations it could take (made with the best 25: --actions or the studio's list chooses others)
- list_items: the list fields in the query (sent once for each of its values)
- list_users: the list fields in the query (sent once for each of its values)
- create_folder: the list fields in the query (sent once for each of its values)
- create_user: the list fields in the query (sent once for each of its values)
- list_nodes: the list level in the query (sent once for each of its values)
- list_nodes: the list parent in the query (sent once for each of its values)
- list_nodes: the list ancestor in the query (sent once for each of its values)
- get_file: the list fields in the query (sent once for each of its values)
- get_folder: the list fields in the query (sent once for each of its values)
- get_user: the list fields in the query (sent once for each of its values)
- get_me: the list fields in the query (sent once for each of its values)
- update_file: the list fields in the query (sent once for each of its values)
- update_folder: the list fields in the query (sent once for each of its values)
- update_user: the list fields in the query (sent once for each of its values)
- get_content: its answer is multipart, not JSON: the result is its text
- get_folder_trash: the list fields in the query (sent once for each of its values)

## Files

The **Files on a server or a folder** step (`storage`) and its trigger take a Box credential (or a developer
token, kind `box_token`, for an hour): files by path from All Files, or inside a folder by its id (`folder_id`) -
see [the studio's guide](../../docs/studio.md#stores-and-drives-over-their-own-apis).

## Tested

Recorded exchanges made from the API description's schemas (`tests/`): what each request must send, and an answer that fits. Not run against a live Box account.

```sh
openduct app check apps/box
openduct app test apps/box
```
