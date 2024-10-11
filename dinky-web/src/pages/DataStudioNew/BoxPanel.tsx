import {KeepAlive} from "react-activation";
import React from "react";
import {Panel} from "react-resizable-panels";


export  type BoxPanelProps = {
  key: string,
  children: React.ReactNode
}
export const BoxPanel = (props: BoxPanelProps) => {
  const {children, key} = props
  return (
    <Panel style={{height: '100%'}}>
      <KeepAlive cacheKey={key}>
        {children}
      </KeepAlive>
    </Panel>
  )
}
