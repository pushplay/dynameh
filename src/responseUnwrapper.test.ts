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
                            "Distemper": {"S": "2015-10-13"}
                        }
                    },
                    "Breed": {"S": "Beagle"},
                    "AnimalType": {"S": "Dog"}
                }
            });
            chai.assert.deepEqual(res, {
                Age: 8,
                Colors: ["White", "Brown", "Black"],
                Name: "Fido",
                Vaccinations: {
                    Rabies: ["2009-03-17", "2011-09-21", "2014-07-08"],
                    Distemper: "2015-10-13"
                },
                Breed: "Beagle",
                AnimalType: "Dog"
            });
        });
    });
});
