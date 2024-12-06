
import { postAll } from '@/services/api';
import { API_CONSTANTS } from '@/services/endpoints';
import { l } from '@/utils/intl';
import { ActionType, ProColumns, ProDescriptions, ProTable } from '@ant-design/pro-components';
import { ProDescriptionsItemProps } from '@ant-design/pro-descriptions';
import { Drawer } from 'antd';
import { useRef, useState } from 'react';

export type SavePointData = {
  id: number;
  taskId: number;
  name: string;
  type: string;
  path: string;
  createTime: Date;
};

export const SavePoint = (props: { taskId: number; }) => {
  const {taskId} = props;

  const [row, setRow] = useState<SavePointData>();
  const actionRef = useRef<ActionType>();
  actionRef.current?.reloadAndRest?.();

  const columns: ProDescriptionsItemProps<SavePointData>[] | ProColumns<SavePointData>[] = [
    {
      title: l('pages.task.savePointPath'),
      dataIndex: 'path',
      hideInForm: true,
      hideInSearch: true
    },
    {
      title: l('global.table.createTime'),
      dataIndex: 'createTime',
      valueType: 'dateTime',
      hideInForm: true,
      hideInSearch: true,
      render: (dom: any, entity: SavePointData) => {
        return <a onClick={() => setRow(entity)}>{dom}</a>;
      }
    }
  ];

  return (
    <>
      <ProTable<SavePointData>
        className={'datastudio-theme'}
        actionRef={actionRef}
        rowKey='id'
        request={(params, sorter, filter) =>
          postAll(API_CONSTANTS.GET_SAVEPOINT_LIST, { ...params, sorter, filter })
        }
        params={{ taskId }}
        columns={columns as ProColumns<SavePointData>[]}
        search={false}
      />
      <Drawer
        width={600}
        open={!!row}
        onClose={() => {
          setRow(undefined);
        }}
        closable={false}
      >
        {row?.name && (
          <ProDescriptions<SavePointData>
            column={2}
            title={row?.name}
            request={async () => ({
              data: row || {}
            })}
            params={{
              id: row?.name
            }}
            columns={columns as ProDescriptionsItemProps<SavePointData>[]}
          />
        )}
      </Drawer>
    </>
  );
}
