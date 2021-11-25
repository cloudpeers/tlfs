import { PeerId } from 'local-first'
import * as React from 'react'
import AceEditor from 'react-ace'

import 'ace-builds/src-noconflict/mode-json'
import 'ace-builds/src-noconflict/theme-github'

export type AppProps = {
  peerId: PeerId,
  ownAddresses: string[],
  doc: object,
  onChange: (_: object) => void

}
export const App = ({ peerId, ownAddresses, doc, onChange: outerChange }: AppProps) => {
  const onChange = (value: string) => {
    try {
      const v = JSON.parse(value)
      outerChange(v)
    } catch (e) {
      //
    }
  }
  return (<div>
    <div>{peerId}</div>
    {ownAddresses.map((a, idx) => (<div key={idx}>{a}</div>))}
    <div>
      <AceEditor
        value={JSON.stringify(doc, null, 2)}
        mode="json"
        theme="github"
        onChange={onChange}
        name="UNIQUE_ID_OF_DIV"
        editorProps={{ $blockScrolling: true }}
        showGutter={false}
        highlightActiveLine={true}
        minLines={100}
        setOptions={{
          showFoldWidgets: false,
          showLineNumbers: true,
          tabSize: 2,
          useWorker: false
        }}
      />,</div>
  </div>)
}
