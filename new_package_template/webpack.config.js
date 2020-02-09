const path = require('path');

module.exports = {
  mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
  entry: ['./client/styles/lang/main.scss', './client/styles/lang/main.rtl.scss', './client/main.ts'],
  devtool: 'inline-source-map',
  resolve: {
    extensions: ['.ts', '.js']
  },
  output: {
    filename: './main.js',
    path: path.resolve(__dirname, 'public/js'),
    library: ''
  },
  module: {
    rules: [
      // all files with a `.ts` or `.tsx` extension will be handled by `ts-loader`
      {test: /\.ts$/, loader: "ts-loader"},
      {
        test: /\.scss$/,
        use: [
          {
            loader: 'file-loader',
            options: {
              name: './../css/[name].css'
            }
          },
          {
            loader: 'extract-loader'
          },
          {
            loader: 'css-loader?-url'
          },
          {
            loader: 'sass-loader'
          }
        ]
      }
    ]
  },
  watch: true,
  target: 'node'
};