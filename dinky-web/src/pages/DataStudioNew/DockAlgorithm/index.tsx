/**
 * @inheritDoc
 * @param source @inheritDoc
 * @param target @inheritDoc
 * @param direction @inheritDoc
 * @param floatPosition @inheritDoc
 */
import {BoxData, DropDirection, FloatPosition, PanelData, TabData} from "rc-dock/src/DockData";
import * as LayoutAlgorithm from "./LayoutAlgorithm";
import {DockLayout} from "rc-dock";

export const dockMove = (
  dockLayout: DockLayout,
  source: TabData | PanelData,
  target: string | TabData | PanelData | BoxData | null,
  direction: DropDirection,
  floatPosition?: FloatPosition
) => {
  let layout = dockLayout.getLayout();
  if (direction === 'maximize') {
    layout = LayoutAlgorithm.maximize(layout, source);
    dockLayout.panelToFocus = source.id;
  } else if (direction === 'front') {
    layout = LayoutAlgorithm.moveToFront(layout, source);
  } else {
    layout = LayoutAlgorithm.removeFromLayout(layout, source);
  }

  if (typeof target === 'string') {
    target = dockLayout.find(target, LayoutAlgorithm.Filter.All);
  } else {
    target = LayoutAlgorithm.getUpdatedObject(target); // target might change during removeTab
  }

  if (direction === 'float') {
    let newPanel = LayoutAlgorithm.converToPanel(source);
    newPanel.z = LayoutAlgorithm.nextZIndex(null);
    if (dockLayout.state.dropRect || floatPosition) {
      layout = LayoutAlgorithm.floatPanel(layout, newPanel, dockLayout.state.dropRect || floatPosition);
    } else {
      layout = LayoutAlgorithm.floatPanel(layout, newPanel);
      if (dockLayout._ref) {
        layout = LayoutAlgorithm.fixFloatPanelPos(layout, dockLayout._ref.offsetWidth, dockLayout._ref.offsetHeight);
      }
    }
  } else if (direction === 'new-window') {
    let newPanel = LayoutAlgorithm.converToPanel(source);
    layout = LayoutAlgorithm.panelToWindow(layout, newPanel);
  } else if (target) {
    if ('tabs' in (target as PanelData)) {
      // panel target
      if (direction === 'middle') {
        layout = LayoutAlgorithm.addTabToPanel(layout, source, target as PanelData);
      } else {
        let newPanel = LayoutAlgorithm.converToPanel(source);
        layout = LayoutAlgorithm.dockPanelToPanel(layout, newPanel, target as PanelData, direction);
      }

    } else if ('children' in (target as BoxData)) {
      // box target
      let newPanel = LayoutAlgorithm.converToPanel(source);
      layout = LayoutAlgorithm.dockPanelToBox(layout, newPanel, target as BoxData, direction);
    } else {
      // tab target
      layout = LayoutAlgorithm.addNextToTab(layout, source, target as TabData, direction);
    }
  }
  if (layout !== dockLayout.getLayout()) {
    layout = LayoutAlgorithm.fixLayoutData(layout, dockLayout.props.groups);
    const currentTabId: string = source.hasOwnProperty('tabs') ? (source as PanelData).activeId : (source as TabData).id;
    dockLayout.changeLayout(layout, currentTabId, direction);
  }
  dockLayout.onDragStateChange(false);
}
