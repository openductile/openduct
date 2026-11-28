# Klaviyo

The Klaviyo REST API.

Made by `openduct app from-openapi` from https://github.com/klaviyo/openapi/blob/8e34fa1c3be8/openapi/stable.json (MIT licence) (OpenAPI 3.0) on 2026-11-27. The vendor's API reference: <https://developers.klaviyo.com>.

## Signing in

- **An API key** (`klaviyo`): an API key from Klaviyo's pages for developers; it goes in the header `Authorization`.

## Actions

- **Get Campaign Messages** (`list_campaign_messages`): Returns campaign messages based on filters.
- **Get Object Types** (`list_object_types`): Get all object types in an account.
- **Get Profiles** (`list_profiles`): Get all profiles in an account.
- **Get Tags** (`list_tags`): List all tags in an account.
- **Get Object Schema Relationships** (`list_object_schemas`): Get related object schemas for an object schema *Rate limits*: Burst: 3/s Steady: 60/m Scopes: custom-objects:read OpenAPI Spec.
- **Create Campaign Message** (`create_campaign_message`): Create a campaign message.
- **Create Object Type** (`create_object_type`): Create an object type.
- **Create Object Schema** (`create_object_schema`): Create an object schema.
- **Create Profile** (`create_profile`): Create a new profile.
- **Create Tag** (`create_tag`): Create a tag.
- **Get Campaign Message** (`get_campaign_message`): Get a campaign message by ID.
- **Get Object Type** (`get_object_type`): Get the object type with the given ID.
- **Get Object Schema** (`get_object_schema`): Get the object schema with the given ID.
- **Get Profile** (`get_profile`): Get the profile with the given profile ID.
- **Get Tag** (`get_tag`): Retrieve the tag with the given tag ID.
- **Update Campaign Message** (`update_campaign_message`): Update a campaign message.
- **Update Object Schema** (`update_object_schema`): Update the object schema with the given ID.
- **Update Profile** (`update_profile`): Update the profile with the given profile ID.
- **Update Tag** (`update_tag`): Update the tag with the given tag ID.
- **Get Campaign for Campaign Message** (`get_campaign_message_campaign`): Return the related omnicampaign *Rate limits*: Burst: 10/s Steady: 150/m Scopes: campaigns:read OpenAPI Spec.
- **Get Current Schema for Object Type** (`get_current_schema`): Get current schema for an object type *Rate limits*: Burst: 3/s Steady: 60/m Scopes: custom-objects:read OpenAPI Spec.
- **Get Source Mapping for Object Schema** (`get_object_schema_source_mapping`): Get source mapping for an object schema *Rate limits*: Burst: 3/s Steady: 60/m Scopes: custom-objects:read OpenAPI Spec.
- **Get Conversation for Profile** (`get_conversation`): Get the conversation for a profile with the given profile ID.
- **Get Tag Group for Tag** (`get_tag_tag_group`): Returns the tag group resource for a given tag ID.
- **Delete Object Type** (`delete_object_type`): Delete the object type with the given ID.

## Not done

- 1 operation left out: a file upload (multipart)
- 410 more operations it could take (made with the best 25: --actions or the studio's list chooses others)
- list_campaign_messages: the list fields[campaign-message] in the query (sent once for each of its values)
- list_campaign_messages: the list fields[campaign] in the query (sent once for each of its values)
- list_campaign_messages: the list include in the query (sent once for each of its values)
- list_object_types: the list fields[object-schema] in the query (sent once for each of its values)
- list_object_types: the list fields[object-type] in the query (sent once for each of its values)
- list_object_types: the list include in the query (sent once for each of its values)
- list_profiles: the list additional-fields[profile] in the query (sent once for each of its values)
- list_profiles: the list fields[conversation] in the query (sent once for each of its values)
- list_profiles: the list fields[profile] in the query (sent once for each of its values)
- list_profiles: the list fields[push-token] in the query (sent once for each of its values)
- list_profiles: the list include in the query (sent once for each of its values)
- list_tags: the list fields[tag-group] in the query (sent once for each of its values)
- list_tags: the list fields[tag] in the query (sent once for each of its values)
- list_tags: the list include in the query (sent once for each of its values)
- create_campaign_message: the list fields[campaign-message] in the query (sent once for each of its values)
- create_object_type: the list fields[object-type] in the query (sent once for each of its values)
- create_object_schema: the list fields[object-schema] in the query (sent once for each of its values)
- create_profile: the list additional-fields[profile] in the query (sent once for each of its values)
- create_profile: the list fields[profile] in the query (sent once for each of its values)
- create_tag: the list fields[tag] in the query (sent once for each of its values)
- get_campaign_message: the list fields[campaign-message] in the query (sent once for each of its values)
- get_campaign_message: the list fields[campaign-variation] in the query (sent once for each of its values)
- get_campaign_message: the list fields[campaign] in the query (sent once for each of its values)
- get_campaign_message: the list include in the query (sent once for each of its values)
- get_object_type: the list fields[object-schema] in the query (sent once for each of its values)
- get_object_type: the list fields[object-type] in the query (sent once for each of its values)
- get_object_type: the list fields[profile-object-type] in the query (sent once for each of its values)
- get_object_type: the list include in the query (sent once for each of its values)
- get_object_schema: the list fields[object-schema] in the query (sent once for each of its values)
- get_object_schema: the list fields[profile-object-schema] in the query (sent once for each of its values)
- get_object_schema: the list fields[source-mapping] in the query (sent once for each of its values)
- get_object_schema: the list include in the query (sent once for each of its values)
- get_profile: the list additional-fields[profile] in the query (sent once for each of its values)
- get_profile: the list fields[conversation] in the query (sent once for each of its values)
- get_profile: the list fields[list] in the query (sent once for each of its values)
- get_profile: the list fields[profile] in the query (sent once for each of its values)
- get_profile: the list fields[push-token] in the query (sent once for each of its values)
- get_profile: the list fields[segment] in the query (sent once for each of its values)
- get_profile: the list include in the query (sent once for each of its values)
- get_tag: the list fields[tag-group] in the query (sent once for each of its values)
- get_tag: the list fields[tag] in the query (sent once for each of its values)
- get_tag: the list include in the query (sent once for each of its values)
- update_campaign_message: the list fields[campaign-message] in the query (sent once for each of its values)
- update_object_schema: the list fields[object-schema] in the query (sent once for each of its values)
- update_profile: the list additional-fields[profile] in the query (sent once for each of its values)
- update_profile: the list fields[profile] in the query (sent once for each of its values)
- get_campaign_message_campaign: the list fields[campaign] in the query (sent once for each of its values)
- get_current_schema: the list fields[object-schema] in the query (sent once for each of its values)
- get_object_schema_source_mapping: the list fields[source-mapping] in the query (sent once for each of its values)
- get_conversation: the list fields[conversation] in the query (sent once for each of its values)
- get_tag_tag_group: the list fields[tag-group] in the query (sent once for each of its values)

## Tested

Recorded exchanges made from the API description's schemas (`tests/`): what each request must send, and an answer that fits. Not run against a live Klaviyo account.

```sh
openduct app check apps/klaviyo
openduct app test apps/klaviyo
```
