import React, { useState } from "react";

function App() {
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(false);

  const apiUrl = process.env.REACT_APP_API_URL || "http://localhost:8000";

  const askQuestion = async () => {
    if (!question.trim()) return;

    const newMessages = [...messages, { role: "user", text: question }];
    setMessages(newMessages);
    setLoading(true);

    try {
      const response = await fetch(
        `${apiUrl}/ask?question=${encodeURIComponent(question)}`
      );

      const data = await response.json();

      setMessages([
        ...newMessages,
        { role: "assistant", text: data.response }
      ]);
    } catch (error) {
      setMessages([
        ...newMessages,
        { role: "assistant", text: "Error contacting backend." }
      ]);
    }

    setQuestion("");
    setLoading(false);
  };

  return (
    <div style={{ maxWidth: "800px", margin: "40px auto", fontFamily: "Arial" }}>
      <h1>Databricks Data Assistant</h1>

      <div
        style={{
          border: "1px solid #ddd",
          borderRadius: "8px",
          padding: "20px",
          minHeight: "300px",
          marginBottom: "20px",
          background: "#fafafa"
        }}
      >
        {messages.map((msg, i) => (
          <div key={i} style={{ marginBottom: "12px" }}>
            <strong>{msg.role === "user" ? "You" : "Assistant"}:</strong>
            <div>{msg.text}</div>
          </div>
        ))}

        {loading && <div>Thinking...</div>}
      </div>

      <div style={{ display: "flex", gap: "10px" }}>
        <input
          style={{ flex: 1, padding: "10px" }}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Ask a question about your data..."
          onKeyDown={(e) => {
            if (e.key === "Enter") askQuestion();
          }}
        />

        <button onClick={askQuestion} style={{ padding: "10px 20px" }}>
          Ask
        </button>
      </div>
    </div>
  );
}

export default App;