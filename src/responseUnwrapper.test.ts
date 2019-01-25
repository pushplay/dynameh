import * as chai from "chai";
import * as responseUnwrapper from "./responseUnwrapper";

describe("responseUnwrapper", () => {
    describe("unwrapGetOutput", () => {
        it("unwraps an empty get to a null object", () => {
            const res = responseUnwrapper.unwrapGetOutput({});
            chai.assert.isNull(res);
        });

        it("unwraps a test response item", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    "Age": {"N": "8"},
                    "isTargetGroup": {"BOOL": false},
                    "Colors": {
                        "L": [
                            {"S": "White"},
                            {"S": "Brown"},
                            {"S": "Black"}
                        ]
                    },
                    "Name": {"S": "Fido"},
                    "Vaccinations": {
                        "M": {
                            "Rabies": {
                                "L": [
                                    {"S": "2009-03-17"},
                                    {"S": "2011-09-21"},
                                    {"S": "2014-07-08"}
                                ]
                            },
                            "Distemper": {"S": "2015-10-13"},
                            "RequiredShots": {
                                M: {
                                    "CDV": {"BOOL": true},
                                    "CAV2": {"BOOL": true},
                                    "CPV2": {"BOOL": false}
                                }
                            }
                        }
                    },
                    "Breed": {"S": "Beagle"},
                    "AnimalType": {"S": "Dog"},
                    "BarCode": {"B": "SmVmZkcgd2FzIGhlcmU="}
                }
            });
            chai.assert.deepEqual(res, {
                Age: 8,
                isTargetGroup: false,
                Colors: ["White", "Brown", "Black"],
                Name: "Fido",
                Vaccinations: {
                    Rabies: ["2009-03-17", "2011-09-21", "2014-07-08"],
                    Distemper: "2015-10-13",
                    RequiredShots: {
                        CDV: true,
                        CAV2: true,
                        CPV2: false
                    }
                },
                BarCode: Buffer.from([0x4a, 0x65, 0x66, 0x66, 0x47, 0x20, 0x77, 0x61, 0x73, 0x20, 0x68, 0x65, 0x72, 0x65]),
                Breed: "Beagle",
                AnimalType: "Dog"
            });
        });

        it("unwraps null", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iamnull: {"NULL": true}
                }
            });
            chai.assert.deepEqual(res, {
                iamnull: null
            });
        });

        it("unwraps a Buffer", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iambuffer: {B: "SGVsbG8gV29ybGQ="}
                }
            });
            chai.assert.deepEqual(res, {
                iambuffer: Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64])
            });
        });

        it("unwraps an array of numbers", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iamnumbers: {
                        L: [
                            {N: "2"},
                            {N: "4"}
                        ]
                    }
                }
            });
            chai.assert.deepEqual(res, {
                iamnumbers: [2, 4]
            });
        });

        it("unwraps an array of strings", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iamstrings: {
                        L: [
                            {S: "Giraffe"},
                            {S: "Hippo"},
                            {S: "Zebra"}
                        ]
                    }
                }
            });
            chai.assert.deepEqual(res, {
                iamstrings: ["Giraffe", "Hippo" ,"Zebra"]
            });
        });

        it("unwraps a Set of numbers", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iamnumbers: {NS: ["2", "4"]}
                }
            });
            chai.assert.deepEqual(res, {
                iamnumbers: new Set([2, 4])
            });
        });

        it("unwraps a Set of strings", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iamstrings: {SS: ["Giraffe", "Hippo" ,"Zebra"]}
                }
            });
            chai.assert.deepEqual(res, {
                iamstrings: new Set(["Giraffe", "Hippo" ,"Zebra"])
            });
        });

        it("unwraps a Set of Buffers", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                Item: {
                    iambuffers: {
                        BS: [
                            "SGVsbG8gV29ybGQ=",
                            "SmVmZkcgd2FzIGhlcmU=",
                            "a3d5amlibw=="
                        ]
                    }
                }
            });
            chai.assert.deepEqual(res, {
                iambuffers: new Set([
                    Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64]),
                    Buffer.from([0x4a, 0x65, 0x66, 0x66, 0x47, 0x20, 0x77, 0x61, 0x73, 0x20, 0x68, 0x65, 0x72, 0x65]),
                    Buffer.from([0x6b, 0x77, 0x79, 0x6a, 0x69, 0x62, 0x6f])
                ])
            });
        });
    });

    describe("unwrapBatchGetOutput", () => {
        it("unwraps a response from multiple tables", () => {
            const response = {
                "Responses": {
                    "Forum": [
                        {
                            "Name":{
                                "S":"Amazon DynamoDB"
                            },
                            "Threads":{
                                "N":"5"
                            },
                            "Messages":{
                                "N":"19"
                            },
                            "Views":{
                                "N":"35"
                            }
                        },
                        {
                            "Name":{
                                "S":"Amazon RDS"
                            },
                            "Threads":{
                                "N":"8"
                            },
                            "Messages":{
                                "N":"32"
                            },
                            "Views":{
                                "N":"38"
                            }
                        },
                        {
                            "Name":{
                                "S":"Amazon Redshift"
                            },
                            "Threads":{
                                "N":"12"
                            },
                            "Messages":{
                                "N":"55"
                            },
                            "Views":{
                                "N":"47"
                            }
                        }
                    ],
                    "Thread": [
                        {
                            "Tags":{
                                "SS":["Reads","MultipleUsers"]
                            },
                            "Message":{
                                "S":"How many users can read a single data item at a time? Are there any limits?"
                            }
                        }
                    ]
                },
                "UnprocessedKeys": {
                },
                "ConsumedCapacity": [
                    {
                        "TableName": "Forum",
                        "CapacityUnits": 3
                    },
                    {
                        "TableName": "Thread",
                        "CapacityUnits": 1
                    }
                ]
            };

            const items = responseUnwrapper.unwrapBatchGetOutput(response);
            chai.assert.deepEqual(items, [
                {
                    Name: "Amazon DynamoDB",
                    Threads: 5,
                    Messages: 19,
                    Views: 35
                },
                {
                    Name: "Amazon RDS",
                    Threads: 8,
                    Messages: 32,
                    Views: 38
                },
                {
                    Name: "Amazon Redshift",
                    Threads: 12,
                    Messages: 55,
                    Views: 47
                },
                {
                    Tags: new Set(["Reads", "MultipleUsers"]),
                    Message: "How many users can read a single data item at a time? Are there any limits?"
                }
            ])
        });
    });

    describe("unwrapScanOutput", () => {
        it("unwraps a response", () => {
            const response = {
                "ConsumedCapacity": {
                    "CapacityUnits": 0.5,
                    "TableName": "Reply"
                },
                "Count": 2,
                "Items": [
                    {
                        "PostedBy": {
                            "S": "joe@example.com"
                        },
                        "ReplyDateTime": {
                            "S": "20130320115336"
                        },
                        "Id": {
                            "S": "Amazon DynamoDB#How do I update multiple items?"
                        },
                        "Message": {
                            "S": "Have you looked at BatchWriteItem?"
                        }
                    },
                    {
                        "PostedBy": {
                            "S": "joe@example.com"
                        },
                        "ReplyDateTime": {
                            "S": "20130320115347"
                        },
                        "Id": {
                            "S": "Amazon DynamoDB#How do I update multiple items?"
                        },
                        "Message": {
                            "S": "BatchWriteItem is documented in the Amazon DynamoDB API Reference."
                        }
                    }
                ],
                "ScannedCount": 4
            };

            const items = responseUnwrapper.unwrapScanOutput(response);
            chai.assert.deepEqual(items, [
                {
                    PostedBy: "joe@example.com",
                    ReplyDateTime: "20130320115336",
                    Id: "Amazon DynamoDB#How do I update multiple items?",
                    Message: "Have you looked at BatchWriteItem?"
                },
                {
                    PostedBy: "joe@example.com",
                    ReplyDateTime: "20130320115347",
                    Id: "Amazon DynamoDB#How do I update multiple items?",
                    Message: "BatchWriteItem is documented in the Amazon DynamoDB API Reference."
                }
            ])
        });
    });

    describe("unwrapQueryOutput", () => {
        it("unwraps a response", () => {
            const response = {
                "ConsumedCapacity": {
                    "CapacityUnits": 1,
                    "TableName": "Reply"
                },
                "Count": 2,
                "Items": [
                    {
                        "ReplyDateTime": {"S": "2015-02-18T20:27:36.165Z"},
                        "PostedBy": {"S": "User A"},
                        "Id": {"S": "Amazon DynamoDB#DynamoDB Thread 1"}
                    },
                    {
                        "ReplyDateTime": {"S": "2015-02-25T20:27:36.165Z"},
                        "PostedBy": {"S": "User B"},
                        "Id": {"S": "Amazon DynamoDB#DynamoDB Thread 1"}
                    }
                ],
                "ScannedCount": 2
            };

            const items = responseUnwrapper.unwrapQueryOutput(response);
            chai.assert.deepEqual(items, [
                {
                    ReplyDateTime: "2015-02-18T20:27:36.165Z",
                    PostedBy: "User A",
                    Id: "Amazon DynamoDB#DynamoDB Thread 1"
                },
                {
                    ReplyDateTime: "2015-02-25T20:27:36.165Z",
                    PostedBy: "User B",
                    Id: "Amazon DynamoDB#DynamoDB Thread 1"
                }
            ]);
        });
    });
});
