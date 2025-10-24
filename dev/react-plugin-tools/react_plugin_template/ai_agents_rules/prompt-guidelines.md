# React Plugin Prompt Guidelines for Beginners

Welcome to your new Airflow React plugin project!  
This guide will help you understand on how you can prompt better, use the AI rules in an effective, beginner friendly manner and even edit the AI rules to match your project's needs.

---

## What Are Prompts and AI Rules?

- **Prompts** are instructions, questions or maybe detailed descriptions related to your React code base issue's description which you give (explain) to AI models or agents (like Copilot, ChatGPT, etc.) to help automate, review your code base, or help you with the code generation and content.
- **AI Rules** help keep your plugin smart, ethical, and easy to use.  
  They can check code style, suggest improvements, and even help debug! 
  For example:
  ```jsx
  // AI can help improve this code
  function Button() {
    return <button>Click</button>
  }
  
  // AI suggestion: Add props and aria-label
  function Button({ onClick, label }) {
    return (
      <button onClick={onClick} aria-label={label}>
        {label}
      </button>
    )
  }
  ```

---

## How to Use This Guide

**Read through each section below for better understanding.**
1. **Try out the examples** by copying, pasting, and editing them in your code or markdown files.
2. **Don't be afraid to ask questions!** Everyone hesitates in the beginning and thats okay! Just know that everyone was a beginner once - you can ask for help:
   - Write comments in your code with questions
   - Open GitHub issues to get community help
   - Use markdown files to document your questions
   - Join the Discord community for help and support from the core contributors and mentors.

   Remember: The only "silly" question is the one you don't ask! 

---

## Setting Up Your React Plugin

1. **Install Node.js and pnpm**  
   If you haven’t yet, download Node.js and install pnpm:
   ```
   npm install -g pnpm
   ```

2. **Install dependencies**
   ```
   pnpm install
   ```

3. **Start the development server**
   ```
   pnpm dev
   ```
   Open the link shown in your terminal to view your plugin live!

---

## Writing Good Prompts for AI Agents

### Example 1: Asking for Code Help
```

How do I display a button in React that says "Click Me"?
```

### Example 2: Requesting a Review
```
Can you review my React component for best practices and accessibility?
```

### Example 3: Fixing an Error
```
Why am I seeing 'undefined is not a function' in my React code?
```

### Example 4: Generating Documentation
```
Write a README section for my React plugin that explains how users can install and use it.
```

---

## Beginner-Friendly React Tips

- **JSX is just HTML in JavaScript!**
  ```jsx
  <button>Click Me</button>
  ```

- **State lets you remember stuff!**
  ```jsx
  const [count, setCount] = useState(0);
  ```

- **Props pass info to components!**
  ```jsx
  <Welcome name="Alice" />
  ```

- **Use helpful comments:**
  ```jsx
  // This button increases the count
  <button onClick={() => setCount(count + 1)}>Increase</button>
  ```

---

## How to Edit AI Rules and Prompts

- Open any markdown file in the `AIagentsrules` folder.
- Add your own prompts, rules, or checklists.
- Save and refresh your project to see changes.

---

## Further Learning

Here are a couple of links you can refer to if you want to have a better understanding of React. 

- [React Docs](https://react.dev/)
- [Airflow Docs](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html)
- [Copilot Prompting Guide](https://docs.github.com/en/copilot)

---

## Kindly Note

React is beginner-friendly and fun.  
**Don’t hesitate to experiment, ask for help, and make mistakes! Learning is a part of journey!**  
Happy coding! 
