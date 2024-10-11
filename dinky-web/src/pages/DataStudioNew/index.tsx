/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import {TabData} from 'rc-dock';
import React, {Suspense, useRef, useState} from 'react';
import {PageContainer} from '@ant-design/pro-layout';
import 'rc-dock/dist/rc-dock.css';
import {Col, Input, Row, Tabs, theme} from 'antd';
import FooterContainer from '@/pages/DataStudio/FooterContainer';
import Toolbar from '@/pages/DataStudioNew/Toolbar';
import {RightContextMenuState} from '@/pages/DataStudioNew/data.d';
import {
  getDockPositionByToolbarPosition,
  handleRightClick,
  InitContextMenuPosition
} from '@/pages/DataStudioNew/function';
import RightContextMenu, {useRightMenuItem} from '@/pages/DataStudioNew/RightContextMenu';
import {MenuInfo} from 'rc-menu/es/interface';
import {TestRoutes, ToolbarRoutes} from '@/pages/DataStudioNew/Toolbar/ToolbarRoute';
import {ToolbarPosition, ToolbarRoute} from '@/pages/DataStudioNew/Toolbar/data.d';
import {connect} from "umi";
import {LayoutState} from "@/pages/DataStudioNew/model";
import {mapDispatchToProps} from "@/pages/DataStudioNew/DvaFunction";
import {getUUID} from "rc-select/es/hooks/useId";
import {Panel, PanelGroup, PanelResizeHandle} from "react-resizable-panels";
import {AliveScope, KeepAlive} from "react-activation";
import {Project} from "@/pages/DataStudioNew/Toolbar/Project";
import {ImperativePanelHandle} from "react-resizable-panels/src/Panel";

const {useToken} = theme;
const DataStudioNew: React.FC = (props: any) => {
  const {
    layoutState,
    handleToolbarShowDesc,
    saveToolbarLayout,
    handleToolbarClick,
    handleLayoutChange,
    addCenterTab
  } = props
  const {token} = useToken();

  const menuItem = useRightMenuItem({layoutState});
  // 右键弹出框状态
  const [rightContextMenuState, setRightContextMenuState] = useState<RightContextMenuState>({
    show: false,
    position: InitContextMenuPosition
  });

  // 工具栏宽度
  const toolbarWidth = layoutState.toolbar.showDesc ? 60 : 30;

  //  右键菜单handle
  const rightContextMenuHandle = (e: any) => handleRightClick(e, setRightContextMenuState);

  const handleMenuClick = (values: MenuInfo) => {
    setRightContextMenuState((prevState) => ({...prevState, show: false}));

    switch (values.key) {
      case 'showToolbarDesc':
      case 'hideToolbarDesc':
        handleToolbarShowDesc()
        break;
      case "saveLayout":
        const uuid = getUUID();
        addCenterTab({
          id: "123" + uuid,
          title: "123" + uuid,
          tabType: 'code'
        })
        break;
    }
  };

  const toolbarOnClick = (route: ToolbarRoute) => {

    handleToolbarClick({
      key: route.key,
      position: route.position
    })
  };

  // 保存工具栏按钮位置布局
  const saveToolbarLayoutHandle = (position: ToolbarPosition, list: string[]) => {
    //todo 思考：当工具栏布局更新时，选择的tab是否需要更新到对应的位置
    const currentSelect: string = layoutState.toolbar[position].currentSelect;
    // 如果新的布局中有tab,说明toolbar被移动了
    const addSelect = list.find((x) => !layoutState.toolbar[position].allTabs.includes(x));
    console.log(addSelect, position, list)
    if (addSelect) {
      const tabData = {
        id: addSelect,
        title: ToolbarRoutes.find((x) => x.key === addSelect)!!.title,
        content: <></>,
        group: position
      }
      // 查找被移动的toolbar位置，先删除，再添加
      const getMoveToolbarPosition = (): ToolbarPosition | undefined => {
        if (layoutState.toolbar.leftTop.allTabs.includes(addSelect)) {
          return 'leftTop'
        }
        if (layoutState.toolbar.leftBottom.allTabs.includes(addSelect)) {
          return 'leftBottom'
        }
        if (layoutState.toolbar.right.allTabs.includes(addSelect)) {
          return 'right'
        }
      }
      const moveToolbarPosition = getMoveToolbarPosition()
      if (moveToolbarPosition) {
        if (layoutState.toolbar[moveToolbarPosition].currentSelect === addSelect) {
          if (currentSelect) {
            dockLayout.updateTab(currentSelect, tabData, true)
          } else {
            setTimeout(() => {
              dockLayout.dockMove(tabData, dockLayoutRef.current!!.getLayout().dockbox, getDockPositionByToolbarPosition(position))
            }, 0)
          }
        }
        dockLayout.dockMove((dockLayout.find(addSelect) as TabData), null, 'remove')
      }
    }


    saveToolbarLayout({
      dockLayout: dockLayoutRef.current!!,
      position,
      list
    })
  };
  return (
    <PageContainer
      breadcrumb={undefined}
      title={false}
      childrenContentStyle={{margin: 0, padding: 0}}
    >
      <Row style={{height: 'calc(100vh - 81px)'}}>
        {/*左边工具栏*/}
        <Col
          style={{width: toolbarWidth, height: 'inherit'}}
          flex='none'
          onContextMenu={rightContextMenuHandle}
        >
          {/*左上工具栏*/}
          <Col style={{width: 'inherit', height: '50%'}}>
            <Toolbar
              showDesc={layoutState.toolbar.showDesc}
              position={'leftTop'}
              onClick={toolbarOnClick}
              toolbarSelect={layoutState.toolbar.leftTop}
              saveToolbarLayout={saveToolbarLayoutHandle}
            />
          </Col>

          {/*左下工具栏*/}
          <Col
            style={{
              width: 'inherit',
              height: '50%'
            }}
          >
            <Toolbar
              showDesc={layoutState.toolbar.showDesc}
              position={'leftBottom'}
              onClick={toolbarOnClick}
              toolbarSelect={layoutState.toolbar.leftBottom}
              saveToolbarLayout={saveToolbarLayoutHandle}
            />
          </Col>
        </Col>

        {/* 中间内容栏*/}
        <Col style={{height: 'inherit'}} flex='auto'>
          <AliveScope>
            <PanelGroup style={{position: 'absolute'}} direction="vertical" onLayout={(sizes: number[]) => {
              console.log(sizes)
            }}>
              <Panel >
                <PanelGroup direction="horizontal">
                  {layoutState.toolbar.leftTop.allOpenTabs.length > 0 && (
                    <>
                      <Panel  style={{height:'100%'}}>
                        <KeepAlive cacheKey={layoutState.toolbar.leftTop.currentSelect} saveScrollPosition={'screen'} style={{height:'100%'}}>
                           {TestRoutes[layoutState.toolbar.leftTop.currentSelect]}
                        </KeepAlive>
                      </Panel>
                      <PanelResizeHandle/>
                    </>
                  )}
                  <Panel>
                    {TestRoutes["quick-start"]}
                  </Panel>
                  {layoutState.toolbar.right.allOpenTabs.length > 0 && (
                    <>
                      <PanelResizeHandle/>
                      <Panel>
                        right
                      </Panel>
                    </>
                  )}
                </PanelGroup>
              </Panel>
              {layoutState.toolbar.leftBottom.allOpenTabs.length > 0 && (
                <>
                  <PanelResizeHandle/>
                  <Panel>
                    bottom
                  </Panel>
                </>
              )}

            </PanelGroup>
          </AliveScope>
        </Col>

        {/*右边工具栏*/}
        <Col
          style={{width: toolbarWidth, height: 'inherit'}}
          flex='none'
          onContextMenu={rightContextMenuHandle}
        >
          <Toolbar
            showDesc={layoutState.toolbar.showDesc}
            position={'right'}
            onClick={toolbarOnClick}
            toolbarSelect={layoutState.toolbar.right}
            saveToolbarLayout={saveToolbarLayoutHandle}
          />
        </Col>
      </Row>

      {/*@ts-ignore*/}
      <FooterContainer token={token}/>

      {/*右键菜单*/}
      <RightContextMenu
        contextMenuPosition={rightContextMenuState.position}
        open={rightContextMenuState.show}
        openChange={() => setRightContextMenuState((prevState) => ({...prevState, show: false}))}
        items={menuItem}
        onClick={handleMenuClick}
      />
    </PageContainer>
  );
};

export default connect(
  ({DataStudio}: { DataStudio: LayoutState }) => ({
    layoutState: DataStudio
  }), mapDispatchToProps)(DataStudioNew);

// export default DataStudioNew
