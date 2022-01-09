
const path = require('path');

module.exports = {
    mode: 'development',
    entry: {
      main: './src/index.ts',
      specs: './spec/index.ts'
    },
    devtool: 'inline-source-map',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
                include: [path.resolve(__dirname, "src"), path.resolve(__dirname, "spec")]
            },
        ],
    },
    resolve: {
        extensions: [ '.tsx', '.ts', '.js' ],
    },
    output: {
        filename: '[name].js',
        path: path.resolve(__dirname, 'dist'),
        library: "sqlengine",
    },
};
