using NJsonSchema.Annotations;

namespace SchemaRegistry.Events;

/// <summary>Message class used for testing</summary>
/// <remarks>
/// """
/// {
/// 	"$schema": "http://json-schema.org/draft-07/schema#",
/// 	"additionalProperties": false,
/// 	"description": "Sample schema to help you get started.",
/// 	"properties": {
/// 	"Id": {
/// 	    "description": "Identifier",
/// 	    "format": "uuid",
/// 	    "type": "string"
/// 	},
/// 	"MessageNumber": {
/// 	    "description": "Number of message",
/// 	    "format": "int32",
/// 	    "type": "integer"
/// 	}
/// 	},
/// 	"required": [
/// 		"Id",
/// 		"MessageNumber"
/// 	],
/// 	"title": "SimpleEvent",
/// 	"type": "object"
/// }
/// """
///</remarks>
public class SimpleEvent : IWithId
{
    public Guid Id { get; set; }
    public int EventNumber { get; set; }
}
