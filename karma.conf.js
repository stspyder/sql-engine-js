module.exports = function(config) {
    config.set({
        frameworks: ["jasmine", "karma-typescript"],
        files: [
            "src/**/*.ts",
            "spec/**/*.ts"
        ],
        karmaTypescriptConfig: {
           compilerOptions: {
              module: "commonjs"
           },
           tsconfig: "./tsconfig.json",
        },
        preprocessors: {
            "**/*.ts": "karma-typescript" // *.tsx for React Jsx
        },
        reporters: ["progress", "karma-typescript"],
        browsers: ["Chrome"]
    });
};
