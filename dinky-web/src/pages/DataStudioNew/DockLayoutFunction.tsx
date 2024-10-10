import {BoxData} from "rc-dock/es";
import {LayoutState} from "@/pages/DataStudioNew/model";
import {ToolbarPosition, ToolbarRoute} from "@/pages/DataStudioNew/Toolbar/data.d";
import {TestRoutes} from "@/pages/DataStudioNew/Toolbar/ToolbarRoute";
import {PanelData} from "rc-dock/es/DockData";

export const createNewPanel = (state: LayoutState, route: ToolbarRoute) => {
    const boxData: PanelData = {
        size: 600,
        tabs: [
            {
                id: route.key,
                content: TestRoutes[route?.key],
                title: route.title,
                group: route.position
            }
        ]
    }
    if (route.position == "right") {
        const box = getBox(state.layoutData.dockbox.children[0] as BoxData)
        box.children.push(boxData)
        state.layoutData.dockbox.children[0] = box
    } else if (route.position === 'leftTop') {
        if ((state.layoutData.dockbox.children[0] as BoxData).children) {
            (state.layoutData.dockbox.children[0] as BoxData).children = [boxData, ...(state.layoutData.dockbox.children[0] as BoxData).children]
        } else {
            (state.layoutData.dockbox.children as BoxData[]) = [boxData, ...(state.layoutData.dockbox.children as BoxData[])]
        }
    }
}


const getBox = (container: BoxData | PanelData): BoxData => {
    if ('tabs' in container) {
        return {
            mode: 'horizontal',
            size: 200,
            children: [
                container
            ]
        }
    }
    return container
}

export const findToolbarPositionByTabId = (toolbar: LayoutState['toolbar'], tabId: string): ToolbarPosition | undefined => {
    if (toolbar.leftTop.allOpenTabs.includes(tabId)) {
        return 'leftTop'
    } else if (toolbar.leftBottom.allOpenTabs.includes(tabId)) {
        return 'leftBottom'
    } else if (toolbar.right.allOpenTabs.includes(tabId)) {
        return 'right'
    }
    return undefined
}
