import * as chai from "chai";
import * as responseUnwrapper from "./responseUnwrapper";

describe("responseUnwrapper", () => {
    describe("unwrapGetOutput", () => {
        it("unwraps an empty get to a null object", () => {
            const res = responseUnwrapper.unwrapGetOutput({});
            chai.assert.isNull(res);
        });

        it("unwraps a response item", () => {
            const res = responseUnwrapper.unwrapGetOutput({
                "Item": {
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
                "Item": {
                    "iamnull": {"NULL": true}
                }
            });
            chai.assert.deepEqual(res, {
                iamnull: null
            });
        });
    });
});
