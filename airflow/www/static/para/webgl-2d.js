/**
 *  WebGL-2D.js - HTML5 Canvas2D API in a WebGL context
 *
 *  Created by Corban Brook <corbanbrook@gmail.com> on 2011-03-02.
 *  Amended to by Bobby Richter <secretrobotron@gmail.com> on 2011-03-03
 *  CubicVR.js by Charles Cliffe <cj@cubicproductions.com> on 2011-03-03
 *
 */

/*
 *  Copyright (c) 2011 Corban Brook
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

/**
 * Usage:
 *
 *    var cvs = document.getElementById("myCanvas");
 *
 *    WebGL2D.enable(cvs); // adds "webgl-2d" to cvs
 *
 *    cvs.getContext("webgl-2d");
 *
 */

(function(Math, undefined) {

  // Vector & Matrix libraries from CubicVR.js
  var M_PI = 3.1415926535897932384626433832795028841968;
  var M_TWO_PI = 2.0 * M_PI;
  var M_HALF_PI = M_PI / 2.0;

  function isPOT(value) {
    return value > 0 && ((value - 1) & value) === 0;
  }

  var vec3 = {
    length: function(pt) {
      return Math.sqrt(pt[0] * pt[0] + pt[1] * pt[1] + pt[2] * pt[2]);
    },

    normalize: function(pt) {
      var d = Math.sqrt((pt[0] * pt[0]) + (pt[1] * pt[1]) + (pt[2] * pt[2]));
      if (d === 0) {
        return [0, 0, 0];
      }
      return [pt[0] / d, pt[1] / d, pt[2] / d];
    },

    dot: function(v1, v2) {
      return v1[0] * v2[0] + v1[1] * v2[1] + v1[2] * v2[2];
    },

    angle: function(v1, v2) {
      return Math.acos((v1[0] * v2[0] + v1[1] * v2[1] + v1[2] * v2[2]) / (Math.sqrt(v1[0] * v1[0] + v1[1] * v1[1] + v1[2] * v1[2]) * Math.sqrt(v2[0] * v2[0] + v2[1] * v2[1] + v2[2] * v2[2])));
    },

    cross: function(vectA, vectB) {
      return [vectA[1] * vectB[2] - vectB[1] * vectA[2], vectA[2] * vectB[0] - vectB[2] * vectA[0], vectA[0] * vectB[1] - vectB[0] * vectA[1]];
    },

    multiply: function(vectA, constB) {
      return [vectA[0] * constB, vectA[1] * constB, vectA[2] * constB];
    },

    add: function(vectA, vectB) {
      return [vectA[0] + vectB[0], vectA[1] + vectB[1], vectA[2] + vectB[2]];
    },

    subtract: function(vectA, vectB) {
      return [vectA[0] - vectB[0], vectA[1] - vectB[1], vectA[2] - vectB[2]];
    },

    equal: function(a, b) {
      var epsilon = 0.0000001;
      if ((a === undefined) && (b === undefined)) {
        return true;
      }
      if ((a === undefined) || (b === undefined)) {
        return false;
      }
      return (Math.abs(a[0] - b[0]) < epsilon && Math.abs(a[1] - b[1]) < epsilon && Math.abs(a[2] - b[2]) < epsilon);
    }
  };

  var mat3 = {
    identity: [1.0, 0.0, 0.0,
               0.0, 1.0, 0.0,
               0.0, 0.0, 1.0],

    multiply: function (m1, m2) {
      var m10 = m1[0], m11 = m1[1], m12 = m1[2], m13 = m1[3], m14 = m1[4], m15 = m1[5], m16 = m1[6], m17 = m1[7], m18 = m1[8],
          m20 = m2[0], m21 = m2[1], m22 = m2[2], m23 = m2[3], m24 = m2[4], m25 = m2[5], m26 = m2[6], m27 = m2[7], m28 = m2[8];

      m2[0] = m20 * m10 + m23 * m11 + m26 * m12;
      m2[1] = m21 * m10 + m24 * m11 + m27 * m12;
      m2[2] = m22 * m10 + m25 * m11 + m28 * m12;
      m2[3] = m20 * m13 + m23 * m14 + m26 * m15;
      m2[4] = m21 * m13 + m24 * m14 + m27 * m15;
      m2[5] = m22 * m13 + m25 * m14 + m28 * m15;
      m2[6] = m20 * m16 + m23 * m17 + m26 * m18;
      m2[7] = m21 * m16 + m24 * m17 + m27 * m18;
      m2[8] = m22 * m16 + m25 * m17 + m28 * m18;
    },

    vec2_multiply: function (m1, m2) {
      var mOut = [];
      mOut[0] = m2[0] * m1[0] + m2[3] * m1[1] + m2[6];
      mOut[1] = m2[1] * m1[0] + m2[4] * m1[1] + m2[7];
      return mOut;
    },

    transpose: function (m) {
      return [m[0], m[3], m[6], m[1], m[4], m[7], m[2], m[5], m[8]];
    }
  }; //mat3

  // Transform library from CubicVR.js
  function Transform(mat) {
    return this.clearStack(mat);
  }

  var STACK_DEPTH_LIMIT = 16;

  Transform.prototype.clearStack = function(init_mat) {
    this.m_stack = [];
    this.m_cache = [];
    this.c_stack = 0;
    this.valid = 0;
    this.result = null;

    for (var i = 0; i < STACK_DEPTH_LIMIT; i++) {
      this.m_stack[i] = this.getIdentity();
    }

    if (init_mat !== undefined) {
      this.m_stack[0] = init_mat;
    } else {
      this.setIdentity();
    }
  }; //clearStack

  Transform.prototype.setIdentity = function() {
    this.m_stack[this.c_stack] = this.getIdentity();
    if (this.valid === this.c_stack && this.c_stack) {
      this.valid--;
    }
  };

  Transform.prototype.getIdentity = function() {
    return [1.0, 0.0, 0.0,
            0.0, 1.0, 0.0,
            0.0, 0.0, 1.0];
  };

  Transform.prototype.getResult = function() {
    if (!this.c_stack) {
      return this.m_stack[0];
    }

    var m = mat3.identity;

    if (this.valid > this.c_stack-1) { this.valid = this.c_stack-1; }

    for (var i = this.valid; i < this.c_stack+1; i++) {
      m = mat3.multiply(this.m_stack[i],m);
      this.m_cache[i] = m;
    }

    this.valid = this.c_stack-1;

    this.result = this.m_cache[this.c_stack];

    return this.result;
  };

  Transform.prototype.pushMatrix = function() {
    this.c_stack++;
    this.m_stack[this.c_stack] = this.getIdentity();
  };

  Transform.prototype.popMatrix = function() {
    if (this.c_stack === 0) { return; }
    this.c_stack--;
  };

  var translateMatrix = Transform.prototype.getIdentity();

  Transform.prototype.translate = function(x, y) {
    translateMatrix[6] = x;
    translateMatrix[7] = y;

    mat3.multiply(translateMatrix, this.m_stack[this.c_stack]);

    /*
    if (this.valid === this.c_stack && this.c_stack) {
      this.valid--;
    }
    */
  };

  var scaleMatrix = Transform.prototype.getIdentity();

  Transform.prototype.scale = function(x, y) {
    scaleMatrix[0] = x;
    scaleMatrix[4] = y;

    mat3.multiply(scaleMatrix, this.m_stack[this.c_stack]);

    /*
    if (this.valid === this.c_stack && this.c_stack) {
      this.valid--;
    }
    */
  };

  var rotateMatrix = Transform.prototype.getIdentity();

  Transform.prototype.rotate = function(ang) {
    var sAng, cAng;

    sAng = Math.sin(-ang);
    cAng = Math.cos(-ang);

    rotateMatrix[0] = cAng;
    rotateMatrix[3] = sAng;
    rotateMatrix[1] = -sAng;
    rotateMatrix[4] = cAng;

    mat3.multiply(rotateMatrix, this.m_stack[this.c_stack]);

    /*
    if (this.valid === this.c_stack && this.c_stack) {
      this.valid--;
    }
    */
  };

  var WebGL2D = this.WebGL2D = function WebGL2D(canvas, options) {
    this.canvas         = canvas;
    this.options        = options || {};
    this.gl             = undefined;
    this.fs             = undefined;
    this.vs             = undefined;
    this.shaderProgram  = undefined;
    this.transform      = new Transform();
    this.shaderPool     = [];
    this.maxTextureSize = undefined;

    // Save a reference to the WebGL2D instance on the canvas object
    canvas.gl2d         = this;

    // Store getContext function for later use
    canvas.$getContext  = canvas.getContext;

    // Override getContext function with "webgl-2d" enabled version
    canvas.getContext = (function(gl2d) {
      return function(context) {
        if ((gl2d.options.force || context === "webgl-2d") && !(canvas.width === 0 || canvas.height === 0)) {
          if (gl2d.gl) { return gl2d.gl; }

          var gl = gl2d.gl = gl2d.canvas.$getContext("experimental-webgl");

          gl2d.initShaders();
          gl2d.initBuffers();

          // Append Canvas2D API features to the WebGL context
          gl2d.initCanvas2DAPI();

          gl.viewport(0, 0, gl2d.canvas.width, gl2d.canvas.height);

          // Depth options
          //gl.enable(gl.DEPTH_TEST);
          //gl.depthFunc(gl.LEQUAL);

          // Blending options
          gl.enable(gl.BLEND);
          gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);

          gl2d.maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE);

          return gl;
        } else {
          return gl2d.canvas.$getContext(context);
        }
      };
    }(this));

    this.postInit();
  };

  // Enables WebGL2D on your canvas
  WebGL2D.enable = function(canvas, options) {
    return canvas.gl2d || new WebGL2D(canvas, options);
  };


  // Shader Pool BitMasks, i.e. sMask = (shaderMask.texture+shaderMask.stroke)
  var shaderMask = {
    texture: 1,
    crop: 2,
    path: 4
  };


  // Fragment shader source
  WebGL2D.prototype.getFragmentShaderSource = function getFragmentShaderSource(sMask) {
    var fsSource = [
      "#ifdef GL_ES",
        "precision highp float;",
      "#endif",

      "#define hasTexture " + ((sMask&shaderMask.texture) ? "1" : "0"),
      "#define hasCrop " + ((sMask&shaderMask.crop) ? "1" : "0"),

      "varying vec4 vColor;",

      "#if hasTexture",
        "varying vec2 vTextureCoord;",
        "uniform sampler2D uSampler;",
        "#if hasCrop",
          "uniform vec4 uCropSource;",
        "#endif",
      "#endif",

      "void main(void) {",
        "#if hasTexture",
          "#if hasCrop",
            "gl_FragColor = texture2D(uSampler, vec2(vTextureCoord.x * uCropSource.z, vTextureCoord.y * uCropSource.w) + uCropSource.xy);",
          "#else",
            "gl_FragColor = texture2D(uSampler, vTextureCoord);",
          "#endif",
        "#else",
          "gl_FragColor = vColor;",
        "#endif",
      "}"
    ].join("\n");

    return fsSource;
  };

  WebGL2D.prototype.getVertexShaderSource = function getVertexShaderSource(stackDepth,sMask) {
    var w = 2 / this.canvas.width, h = -2 / this.canvas.height;

    stackDepth = stackDepth || 1;

    var vsSource = [
      "#define hasTexture " + ((sMask&shaderMask.texture) ? "1" : "0"),
      "attribute vec4 aVertexPosition;",

      "#if hasTexture",
      "varying vec2 vTextureCoord;",
      "#endif",

      "uniform vec4 uColor;",
      "uniform mat3 uTransforms[" + stackDepth + "];",

      "varying vec4 vColor;",

      "const mat4 pMatrix = mat4(" + w + ",0,0,0, 0," + h + ",0,0, 0,0,1.0,1.0, -1.0,1.0,0,0);",

      "mat3 crunchStack(void) {",
        "mat3 result = uTransforms[0];",
        "for (int i = 1; i < " + stackDepth + "; ++i) {",
          "result = uTransforms[i] * result;",
        "}",
        "return result;",
      "}",

      "void main(void) {",
        "vec3 position = crunchStack() * vec3(aVertexPosition.x, aVertexPosition.y, 1.0);",
        "gl_Position = pMatrix * vec4(position, 1.0);",
        "vColor = uColor;",
        "#if hasTexture",
          "vTextureCoord = aVertexPosition.zw;",
        "#endif",
      "}"
    ].join("\n");
    return vsSource;
  };


  // Initialize fragment and vertex shaders
  WebGL2D.prototype.initShaders = function initShaders(transformStackDepth,sMask) {
    var gl = this.gl;

    transformStackDepth = transformStackDepth || 1;
    sMask = sMask || 0;
    var storedShader = this.shaderPool[transformStackDepth];

    if (!storedShader) { storedShader = this.shaderPool[transformStackDepth] = []; }
    storedShader = storedShader[sMask];

    if (storedShader) {
      gl.useProgram(storedShader);
      this.shaderProgram = storedShader;
      return storedShader;
    } else {
      var fs = this.fs = gl.createShader(gl.FRAGMENT_SHADER);
      gl.shaderSource(this.fs, this.getFragmentShaderSource(sMask));
      gl.compileShader(this.fs);

      if (!gl.getShaderParameter(this.fs, gl.COMPILE_STATUS)) {
        throw "fragment shader error: "+gl.getShaderInfoLog(this.fs);
      }

      var vs = this.vs = gl.createShader(gl.VERTEX_SHADER);
      gl.shaderSource(this.vs, this.getVertexShaderSource(transformStackDepth,sMask));
      gl.compileShader(this.vs);

      if (!gl.getShaderParameter(this.vs, gl.COMPILE_STATUS)) {
        throw "vertex shader error: "+gl.getShaderInfoLog(this.vs);
      }


      var shaderProgram = this.shaderProgram = gl.createProgram();
      shaderProgram.stackDepth = transformStackDepth;
      gl.attachShader(shaderProgram, fs);
      gl.attachShader(shaderProgram, vs);
      gl.linkProgram(shaderProgram);

      if (!gl.getProgramParameter(shaderProgram, gl.LINK_STATUS)) {
        throw "Could not initialise shaders.";
      }

      gl.useProgram(shaderProgram);

      shaderProgram.vertexPositionAttribute = gl.getAttribLocation(shaderProgram, "aVertexPosition");
      gl.enableVertexAttribArray(shaderProgram.vertexPositionAttribute);

      shaderProgram.uColor   = gl.getUniformLocation(shaderProgram, 'uColor');
      shaderProgram.uSampler = gl.getUniformLocation(shaderProgram, 'uSampler');
      shaderProgram.uCropSource = gl.getUniformLocation(shaderProgram, 'uCropSource');

      shaderProgram.uTransforms = [];
      for (var i=0; i<transformStackDepth; ++i) {
        shaderProgram.uTransforms[i] = gl.getUniformLocation(shaderProgram, 'uTransforms[' + i + ']');
      } //for
      this.shaderPool[transformStackDepth][sMask] = shaderProgram;
      return shaderProgram;
    } //if
  };

  var rectVertexPositionBuffer;
  var rectVertexColorBuffer;

  var pathVertexPositionBuffer;
  var pathVertexColorBuffer;

  // 2D Vertices and Texture UV coords
  var rectVerts = new Float32Array([
      0,0, 0,0,
      0,1, 0,1,
      1,1, 1,1,
      1,0, 1,0
  ]);

  WebGL2D.prototype.initBuffers = function initBuffers() {
    var gl = this.gl;

    rectVertexPositionBuffer  = gl.createBuffer();
    rectVertexColorBuffer     = gl.createBuffer();

    pathVertexPositionBuffer  = gl.createBuffer();
    pathVertexColorBuffer     = gl.createBuffer();

    gl.bindBuffer(gl.ARRAY_BUFFER, rectVertexPositionBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, rectVerts, gl.STATIC_DRAW);
  };

  // Maintains an array of all WebGL2D instances
  WebGL2D.instances = [];

  WebGL2D.prototype.postInit = function() {
    WebGL2D.instances.push(this);
  };

  // Extends gl context with Canvas2D API
  WebGL2D.prototype.initCanvas2DAPI = function initCanvas2DAPI() {
    var gl2d = this,
        gl   = this.gl;


    // Rendering Canvas for text fonts
    var textCanvas    = document.createElement("canvas");
    textCanvas.width  = gl2d.canvas.width;
    textCanvas.height = gl2d.canvas.height;
    var textCtx       = textCanvas.getContext("2d");

    var reRGBAColor = /^rgb(a)?\(\s*(-?[\d]+)(%)?\s*,\s*(-?[\d]+)(%)?\s*,\s*(-?[\d]+)(%)?\s*,?\s*(-?[\d\.]+)?\s*\)$/;
    var reHSLAColor = /^hsl(a)?\(\s*(-?[\d\.]+)\s*,\s*(-?[\d\.]+)%\s*,\s*(-?[\d\.]+)%\s*,?\s*(-?[\d\.]+)?\s*\)$/;
    var reHex6Color = /^#([0-9A-Fa-f]{6})$/;
    var reHex3Color = /^#([0-9A-Fa-f])([0-9A-Fa-f])([0-9A-Fa-f])$/;

    function HSLAToRGBA(h, s, l, a) {
      var r, g, b, m1, m2;

      // Clamp and Normalize values
      h = (((h % 360) + 360) % 360) / 360;
      s = s > 100 ? 1 : s / 100;
      s = s <   0 ? 0 : s;
      l = l > 100 ? 1 : l / 100;
      l = l <   0 ? 0 : l;

      m2 = l <= 0.5 ? l * (s + 1) : l + s - l * s;
      m1 = l * 2 - m2;

      function getHue(value) {
        var hue;

        if (value * 6 < 1) {
          hue = m1 + (m2 - m1) * value * 6;
        } else if (value * 2 < 1) {
          hue = m2;
        } else if (value * 3 < 2) {
          hue = m1 + (m2 - m1) * (2/3 - value) * 6;
        } else {
          hue = m1;
        }

        return hue;
      }

      r = getHue(h + 1/3);
      g = getHue(h);
      b = getHue(h - 1/3);

      return [r, g, b, a];
    }


    // Converts rgb(a) color string to gl color vector
    function colorStringToVec4(value) {
      var result = [], match, channel, isPercent, hasAlpha, alphaChannel, sameType;

      if ((match = reRGBAColor.exec(value))) {
        hasAlpha = match[1], alphaChannel = parseFloat(match[8]);

        if ((hasAlpha && isNaN(alphaChannel)) || (!hasAlpha && !isNaN(alphaChannel))) {
          return false;
        }

        sameType = match[3];

        for (var i = 2; i < 8; i+=2) {
          channel = match[i], isPercent = match[i+1];

          if (isPercent !== sameType) {
            return false;
          }

          // Clamp and normalize values
          if (isPercent) {
            channel = channel > 100 ? 1 : channel / 100;
            channel = channel <   0 ? 0 : channel;
          } else {
            channel = channel > 255 ? 1 : channel / 255;
            channel = channel <   0 ? 0 : channel;
          }

          result.push(channel);
        }

        result.push(hasAlpha ? alphaChannel : 1.0);
      } else if ((match = reHSLAColor.exec(value))) {
        hasAlpha = match[1], alphaChannel = parseFloat(match[5]);
        result = HSLAToRGBA(match[2], match[3], match[4], parseFloat(hasAlpha && alphaChannel ? alphaChannel : 1.0));
      } else if ((match = reHex6Color.exec(value))) {
        var colorInt = parseInt(match[1], 16);
        result = [((colorInt & 0xFF0000) >> 16) / 255, ((colorInt & 0x00FF00) >> 8) / 255, (colorInt & 0x0000FF) / 255, 1.0];
      } else if ((match = reHex3Color.exec(value))) {
        var hexString = "#" + [match[1], match[1], match[2], match[2], match[3], match[3]].join("");
        result = colorStringToVec4(hexString);
      } else if (value.toLowerCase() in colorKeywords) {
        result = colorStringToVec4(colorKeywords[value.toLowerCase()]);
      } else if (value.toLowerCase() === "transparent") {
        result = [0, 0, 0, 0];
      } else {
        // Color keywords not yet implemented, ie "orange", return hot pink
        return false;
      }

      return result;
    }

    function colorVecToString(vec4) {
      return "rgba(" + (vec4[0] * 255) + ", " + (vec4[1] * 255) + ", " + (vec4[2] * 255) + ", " + parseFloat(vec4[3]) + ")";
    }

    var colorKeywords = {
      aliceblue:            "#f0f8ff",
      antiquewhite:         "#faebd7",
      aqua:                 "#00ffff",
      aquamarine:           "#7fffd4",
      azure:                "#f0ffff",
      beige:                "#f5f5dc",
      bisque:               "#ffe4c4",
      black:                "#000000",
      blanchedalmond:       "#ffebcd",
      blue:                 "#0000ff",
      blueviolet:           "#8a2be2",
      brown:                "#a52a2a",
      burlywood:            "#deb887",
      cadetblue:            "#5f9ea0",
      chartreuse:           "#7fff00",
      chocolate:            "#d2691e",
      coral:                "#ff7f50",
      cornflowerblue:       "#6495ed",
      cornsilk:             "#fff8dc",
      crimson:              "#dc143c",
      cyan:                 "#00ffff",
      darkblue:             "#00008b",
      darkcyan:             "#008b8b",
      darkgoldenrod:        "#b8860b",
      darkgray:             "#a9a9a9",
      darkgreen:            "#006400",
      darkkhaki:            "#bdb76b",
      darkmagenta:          "#8b008b",
      darkolivegreen:       "#556b2f",
      darkorange:           "#ff8c00",
      darkorchid:           "#9932cc",
      darkred:              "#8b0000",
      darksalmon:           "#e9967a",
      darkseagreen:         "#8fbc8f",
      darkslateblue:        "#483d8b",
      darkslategray:        "#2f4f4f",
      darkturquoise:        "#00ced1",
      darkviolet:           "#9400d3",
      deeppink:             "#ff1493",
      deepskyblue:          "#00bfff",
      dimgray:              "#696969",
      dodgerblue:           "#1e90ff",
      firebrick:            "#b22222",
      floralwhite:          "#fffaf0",
      forestgreen:          "#228b22",
      fuchsia:              "#ff00ff",
      gainsboro:            "#dcdcdc",
      ghostwhite:           "#f8f8ff",
      gold:                 "#ffd700",
      goldenrod:            "#daa520",
      gray:                 "#808080",
      green:                "#008000",
      greenyellow:          "#adff2f",
      grey:                 "#808080",
      honeydew:             "#f0fff0",
      hotpink:              "#ff69b4",
      indianred:            "#cd5c5c",
      indigo:               "#4b0082",
      ivory:                "#fffff0",
      khaki:                "#f0e68c",
      lavender:             "#e6e6fa",
      lavenderblush:        "#fff0f5",
      lawngreen:            "#7cfc00",
      lemonchiffon:         "#fffacd",
      lightblue:            "#add8e6",
      lightcoral:           "#f08080",
      lightcyan:            "#e0ffff",
      lightgoldenrodyellow: "#fafad2",
      lightgrey:            "#d3d3d3",
      lightgreen:           "#90ee90",
      lightpink:            "#ffb6c1",
      lightsalmon:          "#ffa07a",
      lightseagreen:        "#20b2aa",
      lightskyblue:         "#87cefa",
      lightslategray:       "#778899",
      lightsteelblue:       "#b0c4de",
      lightyellow:          "#ffffe0",
      lime:                 "#00ff00",
      limegreen:            "#32cd32",
      linen:                "#faf0e6",
      magenta:              "#ff00ff",
      maroon:               "#800000",
      mediumaquamarine:     "#66cdaa",
      mediumblue:           "#0000cd",
      mediumorchid:         "#ba55d3",
      mediumpurple:         "#9370d8",
      mediumseagreen:       "#3cb371",
      mediumslateblue:      "#7b68ee",
      mediumspringgreen:    "#00fa9a",
      mediumturquoise:      "#48d1cc",
      mediumvioletred:      "#c71585",
      midnightblue:         "#191970",
      mintcream:            "#f5fffa",
      mistyrose:            "#ffe4e1",
      moccasin:             "#ffe4b5",
      navajowhite:          "#ffdead",
      navy:                 "#000080",
      oldlace:              "#fdf5e6",
      olive:                "#808000",
      olivedrab:            "#6b8e23",
      orange:               "#ffa500",
      orangered:            "#ff4500",
      orchid:               "#da70d6",
      palegoldenrod:        "#eee8aa",
      palegreen:            "#98fb98",
      paleturquoise:        "#afeeee",
      palevioletred:        "#d87093",
      papayawhip:           "#ffefd5",
      peachpuff:            "#ffdab9",
      peru:                 "#cd853f",
      pink:                 "#ffc0cb",
      plum:                 "#dda0dd",
      powderblue:           "#b0e0e6",
      purple:               "#800080",
      red:                  "#ff0000",
      rosybrown:            "#bc8f8f",
      royalblue:            "#4169e1",
      saddlebrown:          "#8b4513",
      salmon:               "#fa8072",
      sandybrown:           "#f4a460",
      seagreen:             "#2e8b57",
      seashell:             "#fff5ee",
      sienna:               "#a0522d",
      silver:               "#c0c0c0",
      skyblue:              "#87ceeb",
      slateblue:            "#6a5acd",
      slategray:            "#708090",
      snow:                 "#fffafa",
      springgreen:          "#00ff7f",
      steelblue:            "#4682b4",
      tan:                  "#d2b48c",
      teal:                 "#008080",
      thistle:              "#d8bfd8",
      tomato:               "#ff6347",
      turquoise:            "#40e0d0",
      violet:               "#ee82ee",
      wheat:                "#f5deb3",
      white:                "#ffffff",
      whitesmoke:           "#f5f5f5",
      yellow:               "#ffff00",
      yellowgreen:          "#9acd32"
    };

    // Maintain drawing state params during gl.save and gl.restore. see saveDrawState() and restoreDrawState()
    var drawState = {}, drawStateStack = [];

    // A fast simple shallow clone
    function cloneObject(obj) {
      var target = {};
      for (var i in obj) {
        if (obj.hasOwnProperty(i)) {
          target[i] = obj[i];
        }
      }
      return target;
    }

    function saveDrawState() {
      var bakedDrawState = {
        fillStyle:                [drawState.fillStyle[0],   drawState.fillStyle[1],   drawState.fillStyle[2],   drawState.fillStyle[3]],
        strokeStyle:              [drawState.strokeStyle[0], drawState.strokeStyle[1], drawState.strokeStyle[2], drawState.strokeStyle[3]],
        globalAlpha:              drawState.globalAlpha,
        globalCompositeOperation: drawState.globalCompositeOperation,
        lineCap:                  drawState.lineCap,
        lineJoin:                 drawState.lineJoin,
        lineWidth:                drawState.lineWidth,
        miterLimit:               drawState.miterLimit,
        shadowColor:              drawState.shadowColor,
        shadowBlur:               drawState.shadowBlur,
        shadowOffsetX:            drawState.shadowOffsetX,
        shadowOffsetY:            drawState.shadowOffsetY,
        textAlign:                drawState.textAlign,
        font:                     drawState.font,
        textBaseline:             drawState.textBaseline
      };

      drawStateStack.push(bakedDrawState);
    }

    function restoreDrawState() {
      if (drawStateStack.length) {
        drawState = drawStateStack.pop();
      }
    }

    // WebGL requires colors as a vector while Canvas2D sets colors as an rgba string
    // These getters and setters store the original rgba string as well as convert to a vector
    drawState.fillStyle = [0, 0, 0, 1]; // default black

    Object.defineProperty(gl, "fillStyle", {
      get: function() { return colorVecToString(drawState.fillStyle); },
      set: function(value) {
        drawState.fillStyle = colorStringToVec4(value) || drawState.fillStyle;
      }
    });

    drawState.strokeStyle = [0, 0, 0, 1]; // default black

    Object.defineProperty(gl, "strokeStyle", {
      get: function() { return colorVecToString(drawState.strokeStyle); },
      set: function(value) {
        drawState.strokeStyle = colorStringToVec4(value) || drawStyle.strokeStyle;
      }
    });

    // WebGL already has a lineWidth() function but Canvas2D requires a lineWidth property
    // Store the original lineWidth() function for later use
    gl.$lineWidth = gl.lineWidth;
    drawState.lineWidth = 1.0;

    Object.defineProperty(gl, "lineWidth", {
      get: function() { return drawState.lineWidth; },
      set: function(value) {
        gl.$lineWidth(value);
        drawState.lineWidth = value;
      }
    });

    // Currently unsupported attributes and their default values
    drawState.lineCap = "butt";

    Object.defineProperty(gl, "lineCap", {
      get: function() { return drawState.lineCap; },
      set: function(value) {
        drawState.lineCap = value;
      }
    });

    drawState.lineJoin = "miter";

    Object.defineProperty(gl, "lineJoin", {
      get: function() { return drawState.lineJoin; },
      set: function(value) {
        drawState.lineJoin = value;
      }
    });

    drawState.miterLimit = 10;

    Object.defineProperty(gl, "miterLimit", {
      get: function() { return drawState.miterLimit; },
      set: function(value) {
        drawState.miterLimit = value;
      }
    });

    drawState.shadowOffsetX = 0;

    Object.defineProperty(gl, "shadowOffsetX", {
      get: function() { return drawState.shadowOffsetX; },
      set: function(value) {
        drawState.shadowOffsetX = value;
      }
    });

    drawState.shadowOffsetY = 0;

    Object.defineProperty(gl, "shadowOffsetY", {
      get: function() { return drawState.shadowOffsetY; },
      set: function(value) {
        drawState.shadowOffsetY = value;
      }
    });

    drawState.shadowBlur = 0;

    Object.defineProperty(gl, "shadowBlur", {
      get: function() { return drawState.shadowBlur; },
      set: function(value) {
        drawState.shadowBlur = value;
      }
    });

    drawState.shadowColor = "rgba(0, 0, 0, 0.0)";

    Object.defineProperty(gl, "shadowColor", {
      get: function() { return drawState.shadowColor; },
      set: function(value) {
        drawState.shadowColor = value;
      }
    });

    drawState.font = "10px sans-serif";

    Object.defineProperty(gl, "font", {
      get: function() { return drawState.font; },
      set: function(value) {
        textCtx.font = value;
        drawState.font = value;
      }
    });

    drawState.textAlign = "start";

    Object.defineProperty(gl, "textAlign", {
      get: function() { return drawState.textAlign; },
      set: function(value) {
        drawState.textAlign = value;
      }
    });

    drawState.textBaseline = "alphabetic";

    Object.defineProperty(gl, "textBaseline", {
      get: function() { return drawState.textBaseline; },
      set: function(value) {
        drawState.textBaseline = value;
      }
    });

    // This attribute will need to control global alpha of objects drawn.
    drawState.globalAlpha = 1.0;

    Object.defineProperty(gl, "globalAlpha", {
      get: function() { return drawState.globalAlpha; },
      set: function(value) {
        drawState.globalAlpha = value;
      }
    });

    // This attribute will need to set the gl.blendFunc mode
    drawState.globalCompositeOperation = "source-over";

    Object.defineProperty(gl, "globalCompositeOperation", {
      get: function() { return drawState.globalCompositeOperation; },
      set: function(value) {
        drawState.globalCompositeOperation = value;
      }
    });

    // Need a solution for drawing text that isnt stupid slow
    gl.fillText = function fillText(text, x, y) {
      /*
      textCtx.clearRect(0, 0, gl2d.canvas.width, gl2d.canvas.height);
      textCtx.fillStyle = gl.fillStyle;
      textCtx.fillText(text, x, y);

      gl.drawImage(textCanvas, 0, 0);
      */
    };

    gl.strokeText = function strokeText() {};

    gl.measureText = function measureText() { return 1; };

    var tempCanvas = document.createElement('canvas');
    var tempCtx = tempCanvas.getContext('2d');

    gl.save = function save() {
      gl2d.transform.pushMatrix();
      saveDrawState();
    };

    gl.restore = function restore() {
      gl2d.transform.popMatrix();
      restoreDrawState();
    };

    gl.translate = function translate(x, y) {
      gl2d.transform.translate(x, y);
    };

    gl.rotate = function rotate(a) {
      gl2d.transform.rotate(a);
    };

    gl.scale = function scale(x, y) {
      gl2d.transform.scale(x, y);
    };

    gl.createImageData = function createImageData(width, height) {
      return tempCtx.createImageData(width, height);
    };

    gl.getImageData = function getImageData(x, y, width, height) {
      var data = tempCtx.createImageData(width, height);
      var buffer = new Uint8Array(width*height*4);
      gl.readPixels(x, y, width, height, gl.RGBA, gl.UNSIGNED_BYTE, buffer);
      var w=width*4, h=height;
      for (var i=0, maxI=h/2; i<maxI; ++i) {
        for (var j=0, maxJ=w; j<maxJ; ++j) {
          var index1 = i * w + j;
          var index2 = (h-i-1) * w + j;
          data.data[index1] = buffer[index2];
          data.data[index2] = buffer[index1];
        } //for
      } //for

      return data;
    };

    gl.putImageData = function putImageData(imageData, x, y) {
      gl.drawImage(imageData, x, y);
    };

    gl.transform = function transform(m11, m12, m21, m22, dx, dy) {
      var m = gl2d.transform.m_stack[gl2d.transform.c_stack];

      m[0] *= m11;
      m[1] *= m21;
      m[2] *= dx;
      m[3] *= m12;
      m[4] *= m22;
      m[5] *= dy;
      m[6] = 0;
      m[7] = 0;
    };

    function sendTransformStack(sp) {
      var stack = gl2d.transform.m_stack;
      for (var i = 0, maxI = gl2d.transform.c_stack + 1; i < maxI; ++i) {
        gl.uniformMatrix3fv(sp.uTransforms[i], false, stack[maxI-1-i]);
      } //for
    }

    gl.setTransform = function setTransform(m11, m12, m21, m22, dx, dy) {
      gl2d.transform.setIdentity();
      gl.transform.apply(this, arguments);
    };

    gl.fillRect = function fillRect(x, y, width, height) {
      var transform = gl2d.transform;
      var shaderProgram = gl2d.initShaders(transform.c_stack+2,0);

      gl.bindBuffer(gl.ARRAY_BUFFER, rectVertexPositionBuffer);
      gl.vertexAttribPointer(shaderProgram.vertexPositionAttribute, 4, gl.FLOAT, false, 0, 0);

      transform.pushMatrix();

      transform.translate(x, y);
      transform.scale(width, height);

      sendTransformStack(shaderProgram);

      gl.uniform4f(shaderProgram.uColor, drawState.fillStyle[0], drawState.fillStyle[1], drawState.fillStyle[2], drawState.fillStyle[3]);

      gl.drawArrays(gl.TRIANGLE_FAN, 0, 4);

      transform.popMatrix();
    };

    gl.strokeRect = function strokeRect(x, y, width, height) {
      var transform = gl2d.transform;
      var shaderProgram = gl2d.initShaders(transform.c_stack + 2,0);

      gl.bindBuffer(gl.ARRAY_BUFFER, rectVertexPositionBuffer);
      gl.vertexAttribPointer(shaderProgram.vertexPositionAttribute, 4, gl.FLOAT, false, 0, 0);

      transform.pushMatrix();

      transform.translate(x, y);
      transform.scale(width, height);

      sendTransformStack(shaderProgram);

      gl.uniform4f(shaderProgram.uColor, drawState.strokeStyle[0], drawState.strokeStyle[1], drawState.strokeStyle[2], drawState.strokeStyle[3]);

      gl.drawArrays(gl.LINE_LOOP, 0, 4);

      transform.popMatrix();
    };

    gl.clearRect = function clearRect(x, y, width, height) {};

    var subPaths = [];

    function SubPath(x, y) {
      this.closed = false;
      this.verts = [x, y, 0, 0];
    }

    // Empty the list of subpaths so that the context once again has zero subpaths
    gl.beginPath = function beginPath() {
      subPaths.length = 0;
    };

    // Mark last subpath as closed and create a new subpath with the same starting point as the previous subpath
    gl.closePath = function closePath() {
      if (subPaths.length) {
        // Mark last subpath closed.
        var prevPath = subPaths[subPaths.length -1], startX = prevPath.verts[0], startY = prevPath.verts[1];
        prevPath.closed = true;

        // Create new subpath using the starting position of previous subpath
        var newPath = new SubPath(startX, startY);
        subPaths.push(newPath);
      }
    };

    // Create a new subpath with the specified point as its first (and only) point
    gl.moveTo = function moveTo(x, y) {
      subPaths.push(new SubPath(x, y));
    };

    gl.lineTo = function lineTo(x, y) {
      if (subPaths.length) {
        subPaths[subPaths.length -1].verts.push(x, y, 0, 0);
      } else {
        // Create a new subpath if none currently exist
        gl.moveTo(x, y);
      }
    };

    gl.quadraticCurveTo = function quadraticCurveTo(cp1x, cp1y, x, y) {};

    gl.bezierCurveTo = function bezierCurveTo(cp1x, cp1y, cp2x, cp2y, x, y) {};

    gl.arcTo = function arcTo() {};

    // Adds a closed rect subpath and creates a new subpath
    gl.rect = function rect(x, y, w, h) {
      gl.moveTo(x, y);
      gl.lineTo(x + w, y);
      gl.lineTo(x + w, y + h);
      gl.lineTo(x, y + h);
      gl.closePath();
    };

    gl.arc = function arc(x, y, radius, startAngle, endAngle, anticlockwise) {};

    function fillSubPath(index) {
      var transform = gl2d.transform;
      var shaderProgram = gl2d.initShaders(transform.c_stack + 2,0);

      var subPath = subPaths[index];
      var verts = subPath.verts;

      gl.bindBuffer(gl.ARRAY_BUFFER, pathVertexPositionBuffer);
      gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(verts), gl.STATIC_DRAW);

      gl.vertexAttribPointer(shaderProgram.vertexPositionAttribute, 4, gl.FLOAT, false, 0, 0);

      transform.pushMatrix();

      sendTransformStack(shaderProgram);

      gl.uniform4f(shaderProgram.uColor, drawState.fillStyle[0], drawState.fillStyle[1], drawState.fillStyle[2], drawState.fillStyle[3]);

      gl.drawArrays(gl.TRIANGLE_FAN, 0, verts.length/4);

      transform.popMatrix();
    }

    gl.fill = function fill() {
      for(var i = 0; i < subPaths.length; i++) {
        fillSubPath(i);
      }
    };

    function strokeSubPath(index) {
      var transform = gl2d.transform;
      var shaderProgram = gl2d.initShaders(transform.c_stack + 2,0);

      var subPath = subPaths[index];
      var verts = subPath.verts;

      gl.bindBuffer(gl.ARRAY_BUFFER, pathVertexPositionBuffer);
      gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(verts), gl.STATIC_DRAW);

      gl.vertexAttribPointer(shaderProgram.vertexPositionAttribute, 4, gl.FLOAT, false, 0, 0);

      transform.pushMatrix();

      sendTransformStack(shaderProgram);

      gl.uniform4f(shaderProgram.uColor, drawState.strokeStyle[0], drawState.strokeStyle[1], drawState.strokeStyle[2], drawState.strokeStyle[3]);

      if (subPath.closed) {
        gl.drawArrays(gl.LINE_LOOP, 0, verts.length/4);
      } else {
        gl.drawArrays(gl.LINE_STRIP, 0, verts.length/4);
      }

      transform.popMatrix();
    }

    gl.stroke = function stroke() {
      for(var i = 0; i < subPaths.length; i++) {
        strokeSubPath(i);
      }
    };

    gl.clip = function clip() {};

    gl.isPointInPath = function isPointInPath() {};

    gl.drawFocusRing = function drawFocusRing() {};



    var imageCache = [], textureCache = [];

    function Texture(image) {
      this.obj   = gl.createTexture();
      this.index = textureCache.push(this);

      imageCache.push(image);

      // we may wish to consider tiling large images like this instead of scaling and
      // adjust appropriately (flip to next texture source and tile offset) when drawing
      if (image.width > gl2d.maxTextureSize || image.height > gl2d.maxTextureSize) {
        var canvas = document.createElement("canvas");

        canvas.width  = (image.width  > gl2d.maxTextureSize) ? gl2d.maxTextureSize : image.width;
        canvas.height = (image.height > gl2d.maxTextureSize) ? gl2d.maxTextureSize : image.height;

        var ctx = canvas.getContext("2d");

        ctx.drawImage(image, 0, 0, image.width, image.height, 0, 0, canvas.width, canvas.height);

        image = canvas;
      }

      gl.bindTexture(gl.TEXTURE_2D, this.obj);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, image);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);

      // Enable Mip mapping on power-of-2 textures
      if (isPOT(image.width) && isPOT(image.height)) {
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
        gl.generateMipmap(gl.TEXTURE_2D);
      } else {
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
      }

      // Unbind texture
      gl.bindTexture(gl.TEXTURE_2D, null);
    }

    gl.drawImage = function drawImage(image, a, b, c, d, e, f, g, h) {
      var transform = gl2d.transform;

      transform.pushMatrix();

      var sMask = shaderMask.texture;
      var doCrop = false;

      //drawImage(image, dx, dy)
      if (arguments.length === 3) {
        transform.translate(a, b);
        transform.scale(image.width, image.height);
      }

      //drawImage(image, dx, dy, dw, dh)
      else if (arguments.length === 5) {
        transform.translate(a, b);
        transform.scale(c, d);
      }

      //drawImage(image, sx, sy, sw, sh, dx, dy, dw, dh)
      else if (arguments.length === 9) {
        transform.translate(e, f);
        transform.scale(g, h);
        sMask = sMask|shaderMask.crop;
        doCrop = true;
      }

      var shaderProgram = gl2d.initShaders(transform.c_stack, sMask);

      var texture, cacheIndex = imageCache.indexOf(image);

      if (cacheIndex !== -1) {
        texture = textureCache[cacheIndex];
      } else {
        texture = new Texture(image);
      }

      if (doCrop) {
        gl.uniform4f(shaderProgram.uCropSource, a/image.width, b/image.height, c/image.width, d/image.height);
      }

      gl.bindBuffer(gl.ARRAY_BUFFER, rectVertexPositionBuffer);
      gl.vertexAttribPointer(shaderProgram.vertexPositionAttribute, 4, gl.FLOAT, false, 0, 0);

      gl.bindTexture(gl.TEXTURE_2D, texture.obj);
      gl.activeTexture(gl.TEXTURE0);

      gl.uniform1i(shaderProgram.uSampler, 0);

      sendTransformStack(shaderProgram);
      gl.drawArrays(gl.TRIANGLE_FAN, 0, 4);

      transform.popMatrix();
    };
  };

}(Math));
