from pydantic import BaseModel
from typing import Annotated, List, Generator
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessageChunk
from langgraph.graph.message import add_messages
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
from observer.tools import query_loki,get_current_datetime_utc,query_prometheus
from observer.prompts import prompts
from dotenv import load_dotenv
import os

load_dotenv()


class  ObserverState(BaseModel):
    messages: Annotated[List[BaseMessage], add_messages] = []
    chart_json: str = ""


class ObserverAgent:
    """
    Agent class for implementing Langgraph agents.

    Attributes:
        name: The name of the agent.
        tools: The tools available to the agent.
        model: The model to use for the agent.
        system_prompt: The system prompt for the agent.
        temperature: The temperature for the agent.
    """
    def __init__(
            self, 
            name: str, 
            tools: List = [query_loki, get_current_datetime_utc,query_prometheus],
            model: str = "claude-3-5-sonnet-20240620", 
            system_prompt: str = "You are a helpful assistant.",
            temperature: float = 0.1
            ):
        self.name = name
        self.tools = tools
        self.model = model
        self.system_prompt = system_prompt
        self.temperature = temperature
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-flash-lite-latest",
            temperature=0.1
        ).bind_tools(self.tools)
        
        self.runnable = self.build_graph()


    def build_graph(self):
        """
        Build the LangGraph application.
        """
        def observer_node(state: ObserverState) -> ObserverState:
            response = self.llm.invoke(
                [SystemMessage(content=self.system_prompt)] +
                state.messages
                )
            state.messages = state.messages + [response]
            return state
        
        def router(state: ObserverState) -> str:
            last_message = state.messages[-1]
            if not last_message.tool_calls:
                return END
            else:
                return "tools"

        builder = StateGraph(ObserverState)

        builder.add_node("chatbot", observer_node)
        builder.add_node("tools", ToolNode(self.tools))

        builder.add_edge(START, "chatbot")
        builder.add_conditional_edges("chatbot", router, ["tools", END])
        builder.add_edge("tools", "chatbot")

        return builder.compile()
    

    # def inspect_graph(self):
    #     """
    #     Visualize the graph using the mermaid.ink API.
    #     """
    #     from IPython.display import display, Image

    #     graph = self.build_graph()
    #     display(Image(graph.get_graph(xray=True).draw_mermaid_png()))


    def invoke(self, message: str, **kwargs) -> str:
        """Synchronously invoke the graph.

        Args:
            message: The user message.

        Returns:
            str: The LLM response.
        """
        result = self.runnable.invoke(
            input = {
                "messages": [HumanMessage(content=message)]
            },
            **kwargs
        )

        return result["messages"][-1].content
    

    # def stream(self, message: str, **kwargs) -> Generator[str, None, None]:
    #     """Synchronously stream the results of the graph run.

    #     Args:
    #         message: The user message.

    #     Returns:
    #         str: The final LLM response or tool call response
    #     """
    #     for message_chunk, metadata in self.runnable.stream(
    #         input = {
    #             "messages": [HumanMessage(content=message)]
    #         },
    #         stream_mode="messages",
    #         **kwargs
    #     ):
    #         if isinstance(message_chunk, AIMessageChunk):
    #             if message_chunk.response_metadata:
    #                 finish_reason = message_chunk.response_metadata.get("finish_reason", "")
    #                 if finish_reason == "tool_calls":
    #                     yield "\n\n"

    #             if message_chunk.tool_call_chunks:
    #                 tool_chunk = message_chunk.tool_call_chunks[0]

    #                 tool_name = tool_chunk.get("name", "")
    #                 args = tool_chunk.get("args", "")

                    
    #                 if tool_name:
    #                     tool_call_str = f"\n\n< TOOL CALL: {tool_name} >\n\n"

    #                 if args:
    #                     tool_call_str = args
    #                 yield tool_call_str
    #             else:
    #                 yield message_chunk.content
    #             continue


# Define and instantiate the agent 
agent = ObserverAgent(
        name="Observerability Agent",
        system_prompt=prompts.scout_system_prompt
        )
graph = agent.build_graph()
