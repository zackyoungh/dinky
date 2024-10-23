import {handleOption} from "@/services/BusinessCrud";
import {API_CONSTANTS} from "@/services/endpoints";

export async function explainSql(title: string, params: any) {
  return handleOption(API_CONSTANTS.EXPLAIN_SQL, title, params);
}
export async function debugTask(title: string, params: any) {
  return handleOption(API_CONSTANTS.DEBUG_TASK, title, params);
}
