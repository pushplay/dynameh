module.exports = {
    root: true,
    parser: "@typescript-eslint/parser",
    plugins: [
        "@typescript-eslint",
    ],
    extends: [
        "eslint:recommended",
        "plugin:@typescript-eslint/eslint-recommended",
        "plugin:@typescript-eslint/recommended",
    ],
    rules: {
        // Using `object` has not caused me any confusion so far.
        "@typescript-eslint/ban-types": "off",

        // Sometimes `any` is the right answer.
        "@typescript-eslint/explicit-module-boundary-types": ["error", {
            allowArgumentsExplicitlyTypedAsAny: true
        }],

        "@typescript-eslint/explicit-function-return-type": ["error", {
            allowExpressions: true,
            allowTypedFunctionExpressions: true
        }],

        "@typescript-eslint/member-delimiter-style": ["error", {
            multiline: {
                delimiter: "semi",
                requireLast: true
            },
            singleline: {
                delimiter: "comma",
                requireLast: false
            }
        }],

        // That's just stupid.
        "@typescript-eslint/no-empty-function": "off",

        // Sometimes `any` is the right answer.
        "@typescript-eslint/no-explicit-any": "off",

        "@typescript-eslint/no-inferrable-types": ["error", {
            ignoreParameters: true
        }],

        "@typescript-eslint/no-use-before-define": ["error", {
            // Functions are hoisted.  This is not a logic error.
            functions: false
        }],

        "@typescript-eslint/no-unused-vars": ["error", {
            // Often useful to document functions.
            args: "none"
        }],

        // Use the logger instead.
        "no-console": "error",

        "no-constant-condition": ["error", {
            // Allow the while(true) pattern.
            checkLoops: false
        }],

        "no-restricted-properties": [2, {
            object: "describe",
            property: "only",
            message: "This is ok for development but should not be checked in."
        }, {
            object: "it",
            property: "only",
            message: "This is ok for development but should not be checked in."
        }],

        // Not everybody understands the regex spec in that level of detail to recognize
        // unnecessary escapes.  Sometimes the extra escape adds clarity.
        "no-useless-escape": "off"
    }
};
