import HtmlWebpackPlugin from 'html-webpack-plugin'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))

export default {
  mode: 'production',
  entry: './src/index.tsx',
  output: {
    filename: 'main.js',
    path: resolve(__dirname, 'dist')
  },
  optimization: {
    minimize: false
  },
  plugins: [new HtmlWebpackPlugin({
    inject: false,
    templateContent: ({ htmlWebpackPlugin }) => `
        <html>
          <head>
          <meta charset="utf-8">
          <title>Collaborative JSON Editing</title>
          <meta name="viewport" content="width=device-width,initial-scale=1">
           ${htmlWebpackPlugin.tags.headTags}
         </head>
         <body>
           <div id="app"></div>
           ${htmlWebpackPlugin.tags.bodyTags}
         </body>
       </html>
     `
  })],
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/
      }
    ]
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js']
  }
}
